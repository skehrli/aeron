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
package io.aeron.test.cluster;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.cluster.service.ClusterClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestClusterClock implements ClusterClock, EpochClock, NanoClock
{
    private final AtomicLong tick = new AtomicLong();
    private final TimeUnit timeUnit;

    @SideEffectFree
    @Impure
    public TestClusterClock()
    {
        this(TimeUnit.MILLISECONDS);
    }

    @SideEffectFree
    public TestClusterClock(final TimeUnit timeUnit)
    {
        this.timeUnit = timeUnit;
    }

    @Pure
    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    @Impure
    public long time()
    {
        return timeUnit.toMillis(tick.get());
    }

    @Impure
    public long nanoTime()
    {
        return timeUnit.toNanos(tick.get());
    }

    @Impure
    public long timeMillis()
    {
        return timeUnit.toMillis(tick.get());
    }

    @Impure
    public long timeMicros()
    {
        return timeUnit.toMicros(tick.get());
    }

    @Impure
    public long timeNanos()
    {
        return timeUnit.toNanos(tick.get());
    }

    @Impure
    public long convertToNanos(final long time)
    {
        return timeUnit.toNanos(time);
    }

    @Impure
    public void update(final long tick, final TimeUnit tickTimeUnit)
    {
        this.tick.set(tickTimeUnit.convert(tick, tickTimeUnit));
    }

    @Impure
    public long increment(final long tick, final TimeUnit tickTimeUnit)
    {
        return this.tick.addAndGet(tickTimeUnit.convert(tick, tickTimeUnit));
    }

    @Impure
    public long increment(final long tick)
    {
        return increment(tick, timeUnit());
    }
}
