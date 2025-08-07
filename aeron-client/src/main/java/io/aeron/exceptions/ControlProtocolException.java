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
package io.aeron.exceptions;

import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import io.aeron.ErrorCode;

/**
 * Indicates an invalid use of the control protocol when sending commands from the client to driver.
 */
public class ControlProtocolException extends AeronException
{
    private static final long serialVersionUID = -6491010363479568113L;

    /**
     * The {@link ErrorCode} indicating more specific issue experienced by the media driver.
     */
    private final ErrorCode code;

    /**
     * Construct an exception to indicate an invalid command has been sent to the media driver.
     *
     * @param code for the type of error.
     * @param msg  providing more detail.
     */
    @SideEffectFree
    @Impure
    public ControlProtocolException(final ErrorCode code, final String msg)
    {
        super(msg);
        this.code = code;
    }

    /**
     * Construct an exception to indicate an invalid command has been sent to the media driver.
     *
     * @param code      for the type of error.
     * @param rootCause of the error.
     */
    @SideEffectFree
    @Impure
    public ControlProtocolException(final ErrorCode code, final Exception rootCause)
    {
        super(rootCause);
        this.code = code;
    }

    /**
     * Construct an exception to indicate an invalid command has been sent to the media driver.
     *
     * @param code      for the type of error.
     * @param msg       providing more detail.
     * @param rootCause of the error.
     */
    @SideEffectFree
    @Impure
    public ControlProtocolException(final ErrorCode code, final String msg, final Exception rootCause)
    {
        super(msg, rootCause);
        this.code = code;
    }

    /**
     * The {@link ErrorCode} indicating more specific issue experienced by the media driver.
     *
     * @return {@link ErrorCode} indicating more specific issue experienced by the media driver.
     */
    @Pure
    public ErrorCode errorCode()
    {
        return code;
    }
}
