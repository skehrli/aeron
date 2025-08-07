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
package io.aeron;

import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Counter stored in a file managed by the media driver which can be observed with AeronStat.
 */
public final class Counter extends AtomicCounter
{
    private static final VarHandle IS_CLOSED_VH;
    static
    {
        try
        {
            IS_CLOSED_VH = MethodHandles.lookup().findVarHandle(Counter.class, "isClosed", boolean.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private volatile boolean isClosed;
    private final long registrationId;
    private final ClientConductor clientConductor;

    @Impure
    Counter(
        final long registrationId,
        final ClientConductor clientConductor,
        final AtomicBuffer buffer,
        final int counterId)
    {
        super(buffer, counterId);

        this.registrationId = registrationId;
        this.clientConductor = clientConductor;
    }

    /**
     * Construct a read-write view of an existing counter.
     *
     * @param countersReader for getting access to the buffers.
     * @param registrationId assigned by the driver for the counter or {@link Aeron#NULL_VALUE} if not known.
     * @param counterId      for the counter to be viewed.
     * @throws AeronException if the id has for the counter has not been allocated.
     */
    @Impure
    public Counter(final CountersReader countersReader, final long registrationId, final int counterId)
    {
        super(countersReader.valuesBuffer(), counterId);

        if (countersReader.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
        {
            throw new AeronException("Counter id is not allocated: " + counterId);
        }

        this.registrationId = registrationId;
        this.clientConductor = null;
    }

    /**
     * Return the registration id used to register this counter with the media driver.
     *
     * @return the registration id used to register this counter with the media driver.
     */
    @Pure
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Close the counter, releasing the resource managed by the media driver if this was the creator of the Counter.
     * <p>
     * This method is idempotent and thread safe.
     */
    @Impure
    public void close()
    {
        if (IS_CLOSED_VH.compareAndSet(this, false, true))
        {
            super.close();
            if (null != clientConductor)
            {
                clientConductor.releaseCounter(this);
            }
        }
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    @Pure
    public boolean isClosed()
    {
        return isClosed;
    }

    @Impure
    void internalClose()
    {
        super.close();
        isClosed = true;
    }

    @Pure
    ClientConductor clientConductor()
    {
        return clientConductor;
    }
}
