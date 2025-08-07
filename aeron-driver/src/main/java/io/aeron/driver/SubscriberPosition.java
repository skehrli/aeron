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
import org.agrona.concurrent.status.Position;

/**
 * Consumption position a subscriber has got to within a {@link SubscriptionLink}.
 */
final class SubscriberPosition
{
    private final SubscriptionLink subscriptionLink;
    private final Subscribable subscribable;
    private final Position position;

    @SideEffectFree
    SubscriberPosition(
        final SubscriptionLink subscriptionLink, final Subscribable subscribable, final Position position)
    {
        this.subscriptionLink = subscriptionLink;
        this.subscribable = subscribable;
        this.position = position;
    }

    @Pure
    Position position()
    {
        return position;
    }

    @Impure
    int positionCounterId()
    {
        return position().id();
    }

    @Pure
    SubscriptionLink subscription()
    {
        return subscriptionLink;
    }

    @Impure
    void addLink(final PublicationImage image)
    {
        subscriptionLink.link(image, position);
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    public String toString()
    {
        return "SubscriberPosition{" +
            "subscriptionLink=" + subscriptionLink +
            ", subscribable=" + subscribable +
            ", position=" + position +
            '}';
    }
}
