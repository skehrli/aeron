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
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_SHORT;

/**
 * Flyweight for general Aeron network protocol header of a message frame.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Frame Length                           |
 *  +---------------------------------------------------------------+
 *  |  Version    |     Flags     |               Type              |
 *  +-------------+---------------+---------------------------------+
 *  |                       Depends on Type                        ...
 * </pre>
 */
public class HeaderFlyweight extends UnsafeBuffer
{
    /**
     * Header type PAD.
     */
    public static final int HDR_TYPE_PAD = 0x00;

    /**
     * Header type DATA.
     */
    public static final int HDR_TYPE_DATA = 0x01;

    /**
     * Header type NAK.
     */
    public static final int HDR_TYPE_NAK = 0x02;

    /**
     * Header type SM.
     */
    public static final int HDR_TYPE_SM = 0x03;

    /**
     * Header type ERR.
     */
    public static final int HDR_TYPE_ERR = 0x04;

    /**
     * Header type SETUP.
     */
    public static final int HDR_TYPE_SETUP = 0x05;

    /**
     * Header type RTT Measurement.
     */
    public static final int HDR_TYPE_RTTM = 0x06;

    /**
     * Header type RESOLUTION.
     */
    public static final int HDR_TYPE_RES = 0x07;

    /**
     * Header type ATS Data.
     */
    public static final int HDR_TYPE_ATS_DATA = 0x08;

    /**
     * Header type ATS Status Message.
     */
    public static final int HDR_TYPE_ATS_SM = 0x09;

    /**
     * Header type ATS Setup.
     */
    public static final int HDR_TYPE_ATS_SETUP = 0x0A;

    /**
     * Header type Response Setup.
     */
    public static final int HDR_TYPE_RSP_SETUP = 0x0B;

    /**
     * Header type EXT.
     */
    public static final int HDR_TYPE_EXT = 0xFFFF;

    /**
     * Default version.
     */
    public static final byte CURRENT_VERSION = 0x0;

    /**
     * Offset in the frame at which the frame length field begins.
     */
    public static final int FRAME_LENGTH_FIELD_OFFSET = 0;

    /**
     * Offset in the frame at which the version field begins.
     */
    public static final int VERSION_FIELD_OFFSET = 4;

    /**
     * Offset in the frame at which the flags field begins.
     */
    public static final int FLAGS_FIELD_OFFSET = 5;

    /**
     * Offset in the frame at which the frame type field begins.
     */
    public static final int TYPE_FIELD_OFFSET = 6;

    /**
     * Minimum length of any Aeron frame.
     */
    public static final int MIN_HEADER_LENGTH = TYPE_FIELD_OFFSET + SIZE_OF_SHORT;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    @Impure
    public HeaderFlyweight()
    {
    }

    /**
     * Construct a flyweight which wraps a {@link UnsafeBuffer} over the frame.
     *
     * @param buffer to wrap for the flyweight.
     */
    @Impure
    public HeaderFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct a flyweight which wraps a {@link ByteBuffer} over the frame.
     *
     * @param buffer to wrap for the flyweight.
     */
    @Impure
    public HeaderFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * The version field value.
     *
     * @return version field value.
     */
    @Impure
    public short version()
    {
        return (short)(getByte(VERSION_FIELD_OFFSET) & 0xFF);
    }

    /**
     * Set the version field value.
     *
     * @param version field value to be set.
     * @return this for a fluent API.
     */
    @Impure
    public HeaderFlyweight version(final short version)
    {
        putByte(VERSION_FIELD_OFFSET, (byte)version);

        return this;
    }

    /**
     * The flags field value.
     *
     * @return the flags field value.
     */
    @Impure
    public short flags()
    {
        return (short)(getByte(FLAGS_FIELD_OFFSET) & 0xFF);
    }

    /**
     * Set the flags field value.
     *
     * @param flags field value.
     * @return this for a fluent API.
     */
    @Impure
    public HeaderFlyweight flags(final short flags)
    {
        putByte(FLAGS_FIELD_OFFSET, (byte)flags);

        return this;
    }

    /**
     * The type field value.
     *
     * @return the type field value.
     */
    @Impure
    public int headerType()
    {
        return getShort(TYPE_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * Set the type field value.
     *
     * @param type field value.
     * @return this for a fluent API.
     */
    @Impure
    public HeaderFlyweight headerType(final int type)
    {
        putShort(TYPE_FIELD_OFFSET, (short)type, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The length of the frame field value.
     *
     * @return length of the frame field value.
     */
    @Impure
    public int frameLength()
    {
        return getInt(FRAME_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the length of the frame field value.
     *
     * @param length field value.
     * @return this for a fluent API.
     */
    @Impure
    public HeaderFlyweight frameLength(final int length)
    {
        putInt(FRAME_LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Convert header flags to an array of chars to be human-readable.
     *
     * @param flags to be converted.
     * @return header flags converted to an array of chars to be human-readable.
     */
    @Impure
    public static char[] flagsToChars(final short flags)
    {
        final char[] chars = new char[]{ '0', '0', '0', '0', '0', '0', '0', '0' };
        final int length = chars.length;
        short mask = (short)(1 << (length - 1));

        for (int i = 0; i < length; i++)
        {
            if ((flags & mask) == mask)
            {
                chars[i] = '1';
            }

            mask >>= 1;
        }

        return chars;
    }

    /**
     * Append header flags to an {@link Appendable} to be human-readable.
     *
     * @param flags      to be converted.
     * @param appendable to append flags to.
     */
    @Impure
    public static void appendFlagsAsChars(final short flags, final Appendable appendable)
    {
        final int length = 8;
        short mask = (short)(1 << (length - 1));

        try
        {
            for (int i = 0; i < length; i++)
            {
                appendable.append((flags & mask) == mask ? '1' : '0');
                mask >>= 1;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
