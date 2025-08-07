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
package io.aeron.driver;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Pure;
import io.aeron.CommonContext;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import org.agrona.concurrent.status.ReadablePosition;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Subscription registration from a client used for liveness tracking.
 */
public abstract class SubscriptionLink implements DriverManagedResource
{
    final long registrationId;
    final int streamId;
    final boolean isSparse;
    final boolean isTether;
    final boolean isResponse;
    final CommonContext.InferableBoolean group;
    final String channel;
    final AeronClient aeronClient;
    final IdentityHashMap<Subscribable, ReadablePosition> positionBySubscribableMap;
    int sessionId;
    boolean hasSessionId;
    boolean reachedEndOfLife = false;

    @Impure
    SubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channel,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        this.registrationId = registrationId;
        this.streamId = streamId;
        this.channel = channel;
        this.aeronClient = aeronClient;
        this.hasSessionId = params.hasSessionId;
        this.sessionId = params.sessionId;
        this.isSparse = params.isSparse;
        this.isTether = params.isTether;
        this.group = params.group;
        this.isResponse = params.isResponse;

        positionBySubscribableMap = new IdentityHashMap<>(hasSessionId ? 1 : 8);
    }

    /**
     * Channel URI the subscription is on.
     *
     * @return channel URI the subscription is on.
     */
    @Pure
    public final String channel()
    {
        return channel;
    }

    /**
     * Stream id the subscription is on.
     *
     * @return stream id the subscription is on.
     */
    @Pure
    public final int streamId()
    {
        return streamId;
    }

    /**
     * Registration id of the subscription.
     *
     * @return registration id of the subscription.
     */
    @Pure
    public final long registrationId()
    {
        return registrationId;
    }

    @Pure
    AeronClient aeronClient()
    {
        return aeronClient;
    }

    @Pure
    final int sessionId()
    {
        return sessionId;
    }

    @Impure
    void sessionId(final int sessionId)
    {
        this.hasSessionId = true;
        this.sessionId = sessionId;
    }

    @Pure
    boolean isResponse()
    {
        return isResponse;
    }

    @Pure
    ReceiveChannelEndpoint channelEndpoint()
    {
        return null;
    }

    @Pure
    boolean isReliable()
    {
        return true;
    }

    @Pure
    boolean isRejoin()
    {
        return true;
    }

    @Pure
    boolean isTether()
    {
        return isTether;
    }

    @Pure
    boolean isSparse()
    {
        return isSparse;
    }

    @Pure
    CommonContext.InferableBoolean group()
    {
        return group;
    }

    @Pure
    boolean hasSessionId()
    {
        return hasSessionId;
    }

    @Impure
    boolean matches(final NetworkPublication publication)
    {
        return false;
    }

    @Pure
    @Impure
    boolean matches(final PublicationImage image)
    {
        return false;
    }

    @Pure
    @Impure
    boolean matches(final IpcPublication publication)
    {
        return false;
    }

    @Pure
    @Impure
    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final SubscriptionParams params)
    {
        return false;
    }

    @Pure
    @Impure
    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        return false;
    }

    @Pure
    boolean isLinked(final Subscribable subscribable)
    {
        return positionBySubscribableMap.containsKey(subscribable);
    }

    @Impure
    void link(final Subscribable subscribable, final ReadablePosition position)
    {
        positionBySubscribableMap.put(subscribable, position);
    }

    @Impure
    void unlink(final Subscribable subscribable)
    {
        positionBySubscribableMap.remove(subscribable);
    }

    @Pure
    @Impure
    boolean isWildcardOrSessionIdMatch(final int sessionId)
    {
        return (!hasSessionId && !isResponse()) || this.sessionId == sessionId;
    }

    @Pure
    @Impure
    boolean supportsMds()
    {
        return false;
    }

    @Impure
    void notifyUnavailableImages(final DriverConductor conductor)
    {
        for (final Map.Entry<Subscribable, ReadablePosition> entry : positionBySubscribableMap.entrySet())
        {
            final Subscribable subscribable = entry.getKey();
            conductor.notifyUnavailableImageLink(subscribable.subscribableRegistrationId(), this);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void close()
    {
        for (final Map.Entry<Subscribable, ReadablePosition> entry : positionBySubscribableMap.entrySet())
        {
            final Subscribable subscribable = entry.getKey();
            final ReadablePosition position = entry.getValue();
            subscribable.removeSubscriber(this, position);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (aeronClient.hasTimedOut())
        {
            reachedEndOfLife = true;
            conductor.cleanupSubscriptionLink(this);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    @Impure
    public String toString()
    {
        return this.getClass().getName() + "{" +
            "registrationId=" + registrationId +
            ", streamId=" + streamId +
            ", sessionId=" + sessionId +
            ", hasSessionId=" + hasSessionId +
            ", isReliable=" + isReliable() +
            ", isSparse=" + isSparse() +
            ", isTether=" + isTether() +
            ", isRejoin=" + isRejoin() +
            ", reachedEndOfLife=" + reachedEndOfLife +
            ", group=" + group +
            ", channel='" + channel + '\'' +
            ", aeronClient=" + aeronClient +
            ", positionBySubscribableMap=" + positionBySubscribableMap +
            '}';
    }
}

