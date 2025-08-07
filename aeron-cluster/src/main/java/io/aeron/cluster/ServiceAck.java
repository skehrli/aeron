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
import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.cluster.client.ClusterException;

import java.util.ArrayDeque;

/**
 * State holder for ACKs from each of the {@link io.aeron.cluster.service.ClusteredService}s.
 */
final class ServiceAck
{
    static final ServiceAck[] EMPTY_SERVICE_ACKS = new ServiceAck[0];

    private final long ackId;
    private final long logPosition;
    private final long relevantId;

    @SideEffectFree
    ServiceAck(final long ackId, final long logPosition, final long relevantId)
    {
        this.logPosition = logPosition;
        this.ackId = ackId;
        this.relevantId = relevantId;
    }

    @Pure
    long ackId()
    {
        return ackId;
    }

    @Pure
    long logPosition()
    {
        return logPosition;
    }

    @Pure
    long relevantId()
    {
        return relevantId;
    }

    @Deterministic
    static boolean hasReached(final long logPosition, final long ackId, final ArrayDeque<ServiceAck>[] queues)
    {
        for (int serviceId = 0, serviceCount = queues.length; serviceId < serviceCount; serviceId++)
        {
            final ServiceAck serviceAck = queues[serviceId].peek();
            if (null == serviceAck)
            {
                return false;
            }

            if (serviceAck.ackId != ackId || serviceAck.logPosition != logPosition)
            {
                throw new ClusterException(
                    "ack out of sequence: expected [ackId=" + ackId + ", logPosition=" + logPosition + "] vs " +
                    "received [ackId=" + serviceAck.ackId + ", logPosition=" + serviceAck.logPosition +
                    ", relevantId=" + serviceAck.relevantId + ", serviceId=" + serviceId + "]");
            }
        }

        return true;
    }

    @Impure
    static void removeHead(final ArrayDeque<ServiceAck>[] queues)
    {
        for (final ArrayDeque<ServiceAck> queue : queues)
        {
            queue.pollFirst();
        }
    }

    @Impure
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static ArrayDeque<ServiceAck>[] newArrayOfQueues(final int serviceCount)
    {
        final ArrayDeque<ServiceAck>[] queues = new ArrayDeque[serviceCount];

        for (int serviceId = 0; serviceId < serviceCount; serviceId++)
        {
            queues[serviceId] = new ArrayDeque<>();
        }

        return queues;
    }

    @Pure
    public String toString()
    {
        return "ServiceAck{" +
            "ackId=" + ackId +
            ", logPosition=" + logPosition +
            ", relevantId=" + relevantId +
            '}';
    }
}
