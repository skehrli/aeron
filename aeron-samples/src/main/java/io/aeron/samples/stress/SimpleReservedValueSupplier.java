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
package io.aeron.samples.stress;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Pure;
import io.aeron.ReservedValueSupplier;
import org.agrona.DirectBuffer;

class SimpleReservedValueSupplier implements ReservedValueSupplier
{
    private long value;
    @Pure
    public long get(final DirectBuffer termBuffer, final int termOffset, final int frameLength)
    {
        return value;
    }

    @Impure
    ReservedValueSupplier set(final long value)
    {
        this.value = value;
        return this;
    }
}
