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
import org.agrona.collections.Long2LongHashMap;

class ReceiverLivenessTracker
{
    private static final int MISSING_VALUE = -1;

    private final Long2LongHashMap lastSmTimestampNsByReceiverIdMap = new Long2LongHashMap(MISSING_VALUE);

    @Pure
    public boolean hasReceivers()
    {
        return !lastSmTimestampNsByReceiverIdMap.isEmpty();
    }

    @Impure
    public void onStatusMessage(final long receiverId, final long nowNs)
    {
        lastSmTimestampNsByReceiverIdMap.put(receiverId, nowNs);
    }

    @Impure
    public boolean onRemoteClose(final long receiverId)
    {
        return MISSING_VALUE != lastSmTimestampNsByReceiverIdMap.remove(receiverId);
    }

    @Impure
    public void onIdle(final long nowNs, final long timeoutNs)
    {
        final long timeoutThresholdNs = nowNs - timeoutNs;
        final Long2LongHashMap.EntryIterator iterator = lastSmTimestampNsByReceiverIdMap.entrySet().iterator();
        while (iterator.hasNext())
        {
            iterator.next();
            if (iterator.getLongValue() <= timeoutThresholdNs)
            {
                iterator.remove();
            }
        }
    }
}
