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
import io.aeron.driver.media.ReceiveChannelEndpoint;

class NetworkSubscriptionLink extends SubscriptionLink
{
    private final boolean isReliable;
    private final boolean isRejoin;
    private final ReceiveChannelEndpoint channelEndpoint;

    @Impure
    NetworkSubscriptionLink(
        final long registrationId,
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        super(registrationId, streamId, channelUri, aeronClient, params);

        this.isReliable = params.isReliable;
        this.isRejoin = params.isRejoin;
        this.channelEndpoint = channelEndpoint;
    }

    @Pure
    boolean isReliable()
    {
        return isReliable;
    }

    @Pure
    boolean isRejoin()
    {
        return isRejoin;
    }

    @Pure
    ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    @Pure
    @Impure
    boolean matches(final PublicationImage image)
    {
        return image.channelEndpoint() == this.channelEndpoint &&
            image.streamId() == this.streamId &&
            isWildcardOrSessionIdMatch(image.sessionId());
    }

    @Pure
    @Impure
    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final SubscriptionParams params)
    {
        return channelEndpoint == this.channelEndpoint &&
            streamId == this.streamId &&
            hasSessionId == params.hasSessionId &&
            isWildcardOrSessionIdMatch(params.sessionId);
    }

    @Pure
    @Impure
    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        return channelEndpoint == this.channelEndpoint &&
            streamId == this.streamId &&
            isWildcardOrSessionIdMatch(sessionId);
    }

    @Pure
    @Impure
    boolean supportsMds()
    {
        return channelEndpoint.hasDestinationControl();
    }

}
