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
package io.aeron.protocol;

import org.checkerframework.dataflow.qual.Impure;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for general Aeron network protocol error frame
 * <pre>
 *    0                   1                   2                   3
 *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * 0  |R|                 Frame Length (varies)                       |
 *    +---------------+---------------+-------------------------------+
 * 4  |   Version     |     Flags     |         Type (=0x04)          |
 *    +---------------+---------------+-------------------------------+
 * 8  |                          Session ID                           |
 *    +---------------------------------------------------------------+
 * 12 |                           Stream ID                           |
 *    +---------------------------------------------------------------+
 * 16 |                          Receiver ID                          |
 *    |                                                               |
 *    +---------------------------------------------------------------+
 * 24 |                           Group Tag                           |
 *    |                                                               |
 *    +---------------------------------------------------------------+
 * 32 |                          Error Code                           |
 *    +---------------------------------------------------------------+
 * 36 |                     Error String Length                       |
 *    +---------------------------------------------------------------+
 * 40 |                         Error String                        ...
 *    +---------------------------------------------------------------+
 *    ...                                                             |
 *    +---------------------------------------------------------------+
 * </pre>
 * @since 1.47.0
 */
public class ErrorFlyweight extends HeaderFlyweight
{
    /**
     * Length of the Error Header.
     */
    public static final int HEADER_LENGTH = 40;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    public static final int SESSION_ID_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    public static final int STREAM_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the receiver-id field begins.
     */
    public static final int RECEIVER_ID_FIELD_OFFSET = 16;

    /**
     * Offset in the frame at which the group-tag field begins.
     */
    public static final int GROUP_TAG_FIELD_OFFSET = 24;

    /**
     * Offset in the frame at which the error code field begins.
     */
    public static final int ERROR_CODE_FIELD_OFFSET = 32;

    /**
     * Offset in the frame at which the error string field begins. Specifically this will be the length of the string
     * using the Agrona buffer standard of using 4 bytes for the length. Followed by the variable bytes for the string.
     */
    public static final int ERROR_STRING_FIELD_OFFSET = 36;

    /**
     * Maximum length that an error message can be. Can be short that this if configuration options have made the MTU
     * smaller. The error message should be truncated to fit within a single MTU.
     */
    public static final int MAX_ERROR_MESSAGE_LENGTH = 1023;

    /**
     * Maximum length of an error frame. Captures the maximum message length and the header length.
     */
    public static final int MAX_ERROR_FRAME_LENGTH = HEADER_LENGTH + MAX_ERROR_MESSAGE_LENGTH;

    /**
     * Flag to indicate that the group tag field is relevant, if not set the value should be ignored.
     */
    public static final int HAS_GROUP_ID_FLAG = 0x08;

    /**
     * Default constructor for the ErrorFlyweight so that it can be wrapped over a buffer later.
     */
    @Impure
    public ErrorFlyweight()
    {
    }

    /**
     * Construct the ErrorFlyweight over an NIO ByteBuffer frame.
     *
     * @param buffer containing the frame.
     */
    @Impure
    public ErrorFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the ErrorFlyweight over an UnsafeBuffer frame.
     *
     * @param buffer containing the frame.
     */
    @Impure
    public ErrorFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }


    /**
     * The session-id for the stream.
     *
     * @return session-id for the stream.
     */
    @Impure
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set session-id for the stream.
     *
     * @param sessionId session-id for the stream.
     * @return this for a fluent API.
     */
    @Impure
    public ErrorFlyweight sessionId(final int sessionId)
    {
        putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The stream-id for the stream.
     *
     * @return stream-id for the stream.
     */
    @Impure
    public int streamId()
    {
        return getInt(STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set stream-id for the stream.
     *
     * @param streamId stream-id for the stream.
     * @return this for a fluent API.
     */
    @Impure
    public ErrorFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The receiver-id for the stream.
     *
     * @return receiver-id for the stream.
     */
    @Impure
    public long receiverId()
    {
        return getLong(RECEIVER_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set receiver-id for the stream.
     *
     * @param receiverId receiver-id for the stream.
     * @return this for a fluent API.
     */
    @Impure
    public ErrorFlyweight receiverId(final long receiverId)
    {
        putLong(RECEIVER_ID_FIELD_OFFSET, receiverId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the group tag for the message.
     *
     * @return group tag for the message.
     */
    @Impure
    public long groupTag()
    {
        return getLong(GROUP_TAG_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Determines if this message has the group tag flag set.
     *
     * @return <code>true</code> if the flag is set false otherwise.
     */
    @Impure
    public boolean hasGroupTag()
    {
        return HAS_GROUP_ID_FLAG == (HAS_GROUP_ID_FLAG & flags());
    }

    /**
     * Set an optional group tag, null indicates the value should not be set. If non-null will set HAS_GROUP_TAG flag
     * on the header. A null value will clear this flag and use a value of 0.
     *
     * @param groupTag optional group tag to be applied to this message.
     * @return this for a fluent API.
     */
    @Impure
    public ErrorFlyweight groupTag(final Long groupTag)
    {
        if (null == groupTag)
        {
            flags((short)(~HAS_GROUP_ID_FLAG & flags()));
        }
        else
        {
            putLong(GROUP_TAG_FIELD_OFFSET, groupTag);
            flags((short)(HAS_GROUP_ID_FLAG | flags()));
        }

        return this;
    }

    /**
     * The error-code for the message.
     *
     * @return error-code for the message.
     */
    @Impure
    public int errorCode()
    {
        return getInt(ERROR_CODE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set error-code for the message.
     *
     * @param errorCode for the message.
     * @return this for a fluent API.
     */
    @Impure
    public ErrorFlyweight errorCode(final int errorCode)
    {
        putInt(ERROR_CODE_FIELD_OFFSET, errorCode, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the error string for the message.
     *
     * @return the error string for the message.
     */
    @Impure
    public String errorMessage()
    {
        return getStringAscii(ERROR_STRING_FIELD_OFFSET);
    }

    /**
     * Set the error string for the message.
     *
     * @param errorMessage the error string in UTF-8.
     * @return this for a fluent API.
     */
    @Impure
    public ErrorFlyweight errorMessage(final String errorMessage)
    {
        final int headerAndMessageLength = putStringAscii(ERROR_STRING_FIELD_OFFSET, errorMessage, LITTLE_ENDIAN);
        frameLength(HEADER_LENGTH + (headerAndMessageLength - STR_HEADER_LEN));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public String toString()
    {
        return "ERROR{" +
            "frame-length=" + frameLength() +
            " version=" + version() +
            " flags=" + String.valueOf(flagsToChars(flags())) +
            " type=" + headerType() +
            " session-id=" + sessionId() +
            " stream-id=" + streamId() +
            " error-code=" + errorCode() +
            " error-message=" + errorMessage() +
            "}";
    }
}
