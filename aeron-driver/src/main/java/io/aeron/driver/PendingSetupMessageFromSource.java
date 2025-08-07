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

import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.driver.media.ReceiveChannelEndpoint;

import java.net.InetSocketAddress;

final class PendingSetupMessageFromSource
{
    private final int sessionId;
    private final int streamId;
    private final int transportIndex;
    private final boolean periodic;
    private final ReceiveChannelEndpoint channelEndpoint;
    private InetSocketAddress controlAddress;

    private long timeOfStatusMessageNs;

    @SideEffectFree
    PendingSetupMessageFromSource(
        final int sessionId,
        final int streamId,
        final int transportIndex,
        final ReceiveChannelEndpoint channelEndpoint,
        final boolean periodic,
        final InetSocketAddress controlAddress)
    {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.transportIndex = transportIndex;
        this.channelEndpoint = channelEndpoint;
        this.periodic = periodic;
        this.controlAddress = controlAddress;
    }

    @Pure
    int sessionId()
    {
        return sessionId;
    }

    @Pure
    int streamId()
    {
        return streamId;
    }

    @Pure
    int transportIndex()
    {
        return transportIndex;
    }

    @Pure
    ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    @Pure
    boolean isPeriodic()
    {
        return periodic;
    }

    @Pure
    @Impure
    boolean shouldElicitSetupMessage()
    {
        return channelEndpoint.dispatcher().shouldElicitSetupMessage();
    }

    @Impure
    void controlAddress(final InetSocketAddress newControlAddress)
    {
        this.controlAddress = newControlAddress;
    }

    @Pure
    InetSocketAddress controlAddress()
    {
        return controlAddress;
    }

    @Pure
    long timeOfStatusMessageNs()
    {
        return timeOfStatusMessageNs;
    }

    @Impure
    void timeOfStatusMessageNs(final long nowNs)
    {
        timeOfStatusMessageNs = nowNs;
    }

    @Impure
    void removeFromDataPacketDispatcher()
    {
        channelEndpoint.dispatcher().removePendingSetup(sessionId, streamId);
    }

    @Pure
    public String toString()
    {
        return "PendingSetupMessageFromSource{" +
            "sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", transportIndex=" + transportIndex +
            ", periodic=" + periodic +
            ", channelEndpoint=" + channelEndpoint +
            ", controlAddress=" + controlAddress +
            ", timeOfStatusMessageNs=" + timeOfStatusMessageNs +
            '}';
    }
}
