/*
 * Copyright 2014-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.response;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Pure;
import io.aeron.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.Objects;
import java.util.function.Function;

/**
 * A basic sample response server that allows the users to specify a simple function to represent the handling of a
 * request and then return a response. This approach will be effective when request processing is very short. For
 * certain types of response servers, e.g. returning a large volume of data from a database, this pattern will be
 * ineffective. For those types of use cases, something more complex, likely involving thread pooling would be required.
 */
public class ResponseServer implements AutoCloseable, Agent
{
    /**
     * Interface to manage callback from the response server onto a session.
     */
    public interface ResponseHandler
    {
        /**
         * Called when a message is received via the request subscription.
         *
         * @param buffer              containing the data.
         * @param offset              at which the data begins.
         * @param length              of the data in bytes.
         * @param header              representing the metadata for the data.
         * @param responsePublication to send responses back to the client.
         * @return <code>true</code> if the message was processed otherwise.
         */
        @Pure
        boolean onMessage(DirectBuffer buffer, int offset, int length, Header header, Publication responsePublication);
    }

    private final Aeron aeron;
    private final Long2ObjectHashMap<ResponseSession> clientToPublicationMap = new Long2ObjectHashMap<>();
    private final OneToOneConcurrentArrayQueue<Image> availableImages =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final OneToOneConcurrentArrayQueue<Image> unavailableImages =
        new OneToOneConcurrentArrayQueue<>(1024);
    private final Function<Image, ResponseHandler> handlerFactory;
    private final int requestStreamId;
    private final int responseStreamId;
    private final ChannelUriStringBuilder requestUriBuilder;
    private final ChannelUriStringBuilder responseUriBuilder;
    private final ControlledFragmentAssembler requestAssembler = new ControlledFragmentAssembler(
        this::onControlledRequestMessage);

    private Subscription serverSubscription;

    /**
     * Constructs the server.
     *
     * @param aeron            connected Aeron client.
     * @param handlerFactory   factor to produce handler instances for each incoming session.
     * @param requestEndpoint  channel to listen for requests and new connections.
     * @param requestStreamId  streamId to listen for requests and new connections.
     * @param responseControl  channel to send responses back on.
     * @param responseStreamId streamId to send responses back on.
     * @param requestChannel   fragment to aid in configuration of the subscription (can be null)
     * @param responseChannel  fragment to aid in configuration of the response publication (can be null)
     */
    @Impure
    public ResponseServer(
        final Aeron aeron,
        final Function<Image, ResponseHandler> handlerFactory,
        final String requestEndpoint,
        final int requestStreamId,
        final String responseControl,
        final int responseStreamId,
        final String requestChannel,
        final String responseChannel)
    {
        this.aeron = aeron;
        this.handlerFactory = handlerFactory;
        this.requestStreamId = requestStreamId;
        this.responseStreamId = responseStreamId;

        Objects.requireNonNull(requestEndpoint, "subscriptionEndpoint must not be null");
        Objects.requireNonNull(responseControl, "responseEndpoint must not be null");

        requestUriBuilder = null == requestChannel ?
            new ChannelUriStringBuilder() : new ChannelUriStringBuilder(requestChannel);
        requestUriBuilder
            .media("udp")
            .endpoint(requestEndpoint)
            .responseEndpoint(responseControl);
        responseUriBuilder = null == responseChannel ?
            new ChannelUriStringBuilder() : new ChannelUriStringBuilder(responseChannel);
        responseUriBuilder
            .media("udp")
            .controlMode("response")
            .controlEndpoint(responseControl);
    }

    /**
     * Poll the server process messages and state.
     *
     * @return amount of work done.
     */
    @Impure
    public int doWork()
    {
        int workCount = 0;

        if (null == serverSubscription)
        {
            serverSubscription = aeron.addSubscription(
                requestUriBuilder.build(),
                requestStreamId,
                this::enqueueAvailableImage,
                this::enqueueUnavailableImage);
            workCount++;
        }

        Image image;
        while (null != (image = availableImages.poll()))
        {
            workCount++;
            getOrCreateSession(image);
        }

        while (null != (image = unavailableImages.poll()))
        {
            workCount++;
            removeSession(image);
        }

        workCount += serverSubscription.controlledPoll(requestAssembler, 1);

        return workCount;
    }

    /**
     * The number of connected clients.
     *
     * @return the number of connected clients.
     */
    @Pure
    public int sessionCount()
    {
        return clientToPublicationMap.size();
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void close()
    {
        CloseHelper.quietClose(serverSubscription);
        clientToPublicationMap.values().forEach(CloseHelper::quietClose);
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    public String roleName()
    {
        return "ResponseServer";
    }

    @Impure
    private void enqueueAvailableImage(final Image image)
    {
        if (!availableImages.offer(image))
        {
            throw new RuntimeException("Unable to enqueue new image");
        }
    }

    @Impure
    private void enqueueUnavailableImage(final Image image)
    {
        if (!unavailableImages.offer(image))
        {
            throw new RuntimeException("Unable to enqueue removed image");
        }
    }

    @Impure
    private ControlledFragmentHandler.Action onControlledRequestMessage(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final ResponseSession session = getOrCreateSession((Image)header.context());
        final boolean processed = session.process(buffer, offset, length, header);

        return processed ? ControlledFragmentHandler.Action.CONTINUE : ControlledFragmentHandler.Action.ABORT;
    }


    @Impure
    private ResponseSession getOrCreateSession(final Image image)
    {
        ResponseSession session = clientToPublicationMap.get(image.correlationId());

        if (null == session)
        {
            final Publication responsePublication = aeron.addPublication(
                responseUriBuilder.responseCorrelationId(image.correlationId()).build(),
                responseStreamId);

            final ResponseHandler handler = handlerFactory.apply(image);
            session = new ResponseSession(responsePublication, handler);

            clientToPublicationMap.put(image.correlationId(), session);
        }

        return session;
    }

    @Impure
    private void removeSession(final Image image)
    {
        requestAssembler.freeSessionBuffer(image.sessionId());
        final ResponseSession session = clientToPublicationMap.remove(image.correlationId());
        CloseHelper.quietClose(session);
    }

    private static final class ResponseSession implements AutoCloseable
    {
        private final Publication publication;
        private final ResponseHandler handler;

        @SideEffectFree
        ResponseSession(final Publication publication, final ResponseHandler handler)
        {
            this.publication = publication;
            this.handler = handler;
        }

        @Pure
        @Impure
        public boolean process(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            return handler.onMessage(buffer, offset, length, header, publication);
        }

        /**
         * {@inheritDoc}
         */
        @Impure
        public void close()
        {
            CloseHelper.close(publication);
        }
    }
}
