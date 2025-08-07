/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.cluster;

import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Impure;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.BitUtil.align;

final class LogPublisher
{
    private static final int SEND_ATTEMPTS = 3;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionMessageHeaderEncoder sessionHeaderEncoder = new SessionMessageHeaderEncoder();
    private final SessionOpenEventEncoder sessionOpenEventEncoder = new SessionOpenEventEncoder();
    private final SessionCloseEventEncoder sessionCloseEventEncoder = new SessionCloseEventEncoder();
    private final TimerEventEncoder timerEventEncoder = new TimerEventEncoder();
    private final ClusterActionRequestEncoder clusterActionRequestEncoder = new ClusterActionRequestEncoder();
    private final NewLeadershipTermEventEncoder newLeadershipTermEventEncoder = new NewLeadershipTermEventEncoder();
    private final UnsafeBuffer sessionHeaderBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    private final BufferClaim bufferClaim = new BufferClaim();

    private final String destinationChannel;
    private ExclusivePublication publication;

    @Impure
    LogPublisher(final String destinationChannel)
    {
        this.destinationChannel = destinationChannel;
        sessionHeaderEncoder.wrapAndApplyHeader(sessionHeaderBuffer, 0, new MessageHeaderEncoder());
    }

    @Impure
    void publication(final ExclusivePublication publication)
    {
        if (null != this.publication)
        {
            this.publication.close();
        }
        this.publication = publication;
    }

    @Pure
    ExclusivePublication publication()
    {
        return publication;
    }

    @Impure
    void disconnect(final ErrorHandler errorHandler)
    {
        if (null != publication)
        {
            CloseHelper.close(errorHandler, publication);
            this.publication = null;
        }
    }

    @Impure
    long position()
    {
        if (null == publication)
        {
            return 0;
        }

        return publication.position();
    }

    @Impure
    int sessionId()
    {
        return publication.sessionId();
    }

    @Impure
    void addDestination(final String followerLogEndpoint)
    {
        if (null != publication)
        {
            publication.asyncAddDestination(ChannelUri.createDestinationUri(destinationChannel, followerLogEndpoint));
        }
    }

    @Impure
    long appendMessage(
        final long leadershipTermId,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        sessionHeaderEncoder
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .timestamp(timestamp);

        int attempts = SEND_ATTEMPTS;
        long position;
        do
        {
            position = publication.offer(sessionHeaderBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);

            if (position > 0)
            {
                break;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);

        return position;
    }

    @Impure
    long appendSessionOpen(final ClusterSession session, final long leadershipTermId, final long timestamp)
    {
        long position;
        final byte[] encodedPrincipal = session.encodedPrincipal();
        final String channel = session.responseChannel();

        sessionOpenEventEncoder
            .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(session.id())
            .correlationId(session.correlationId())
            .timestamp(timestamp)
            .responseStreamId(session.responseStreamId())
            .responseChannel(channel)
            .putEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + sessionOpenEventEncoder.encodedLength();

        int attempts = SEND_ATTEMPTS;
        do
        {
            position = publication.offer(expandableArrayBuffer, 0, length, null);
            if (position > 0)
            {
                break;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);

        return position;
    }

    @Impure
    boolean appendSessionClose(
        final int memberId,
        final ClusterSession session,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        logAppendSessionClose(memberId, session.id(), session.closeReason(), leadershipTermId, timestamp, timeUnit);

        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long position = publication.tryClaim(length, bufferClaim);
            if (position > 0)
            {
                sessionCloseEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(session.id())
                    .timestamp(timestamp)
                    .closeReason(session.closeReason());

                bufferClaim.commit();
                return true;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);

        return false;
    }

    @Impure
    long appendTimer(final long correlationId, final long leadershipTermId, final long timestamp)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH;

        int attempts = SEND_ATTEMPTS;
        long position;
        do
        {
            position = publication.tryClaim(length, bufferClaim);
            if (position > 0)
            {
                timerEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .correlationId(correlationId)
                    .timestamp(timestamp);

                bufferClaim.commit();
                break;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);

        return position;
    }

    @Impure
    boolean appendClusterAction(
        final long leadershipTermId,
        final long timestamp,
        final ClusterAction action,
        final int flags)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionRequestEncoder.BLOCK_LENGTH;
        final int fragmentLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int alignedFragmentLength = align(fragmentLength, FRAME_ALIGNMENT);

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long logPosition = publication.position() + alignedFragmentLength;
            final long position = publication.tryClaim(length, bufferClaim);

            if (position > 0)
            {
                clusterActionRequestEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .action(action)
                    .flags(flags);

                bufferClaim.commit();
                return true;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);

        return false;
    }

    @Impure
    boolean appendNewLeadershipTermEvent(
        final long leadershipTermId,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH;
        final int fragmentLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int alignedFragmentLength = align(fragmentLength, FRAME_ALIGNMENT);

        int attempts = SEND_ATTEMPTS;
        do
        {
            final long logPosition = publication.position() + alignedFragmentLength;
            final long position = publication.tryClaim(length, bufferClaim);

            if (position > 0)
            {
                newLeadershipTermEventEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .termBaseLogPosition(termBaseLogPosition)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId)
                    .timeUnit(ClusterClock.map(timeUnit))
                    .appVersion(appVersion);

                bufferClaim.commit();
                return true;
            }

            checkResult(position, publication);
        }
        while (--attempts > 0);

        return false;
    }

    @Impure
    private static void checkResult(final long position, final Publication publication)
    {
        if (Publication.CLOSED == position)
        {
            throw new ClusterException("log publication is closed");
        }

        if (Publication.MAX_POSITION_EXCEEDED == position)
        {
            throw new ClusterException(
                "log publication at max position: term-length=" + publication.termBufferLength());
        }
    }

    @SideEffectFree
    private static void logAppendSessionClose(
        final int memberId,
        final long id,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    public String toString()
    {
        return "LogPublisher{" +
            "destinationChannel='" + destinationChannel + '\'' +
            '}';
    }
}
