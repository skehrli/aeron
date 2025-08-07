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
package io.aeron.agent;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.dataflow.qual.SideEffectFree;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.agent.CommonEventEncoder.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_BYTE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ArchiveEventEncoder
{
    @SideEffectFree
    private ArchiveEventEncoder()
    {
    }

    @Impure
    static <E extends Enum<E>> int encodeReplaySessionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final @Owning E from,
        final @Owning E to,
        final long id,
        final long recordingId,
        final long position,
        final String reason)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, id, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, recordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, position, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodedLength += encodeStateChange(encodingBuffer, offset + encodedLength, from, to);

        encodedLength += encodeTrailingString(encodingBuffer, offset + encodedLength,
            captureLength + LOG_HEADER_LENGTH - encodedLength, reason);

        return encodedLength;
    }

    @Pure
    @Impure
    static <E extends Enum<E>> int replaySessionStateChangeLength(final @Owning E from, final @Owning E to, final String reason)
    {
        return stateTransitionStringLength(from, to) + (3 * SIZE_OF_LONG) + (SIZE_OF_INT + reason.length());
    }

    @Impure
    static <E extends Enum<E>> int encodeRecordingSessionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final @Owning E from,
        final @Owning E to,
        final long recordingId,
        final long position,
        final String reason)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, recordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, position, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodedLength += encodeStateChange(encodingBuffer, offset + encodedLength, from, to);

        encodedLength += encodeTrailingString(encodingBuffer, offset + encodedLength,
            captureLength + LOG_HEADER_LENGTH - encodedLength, reason);

        return encodedLength;
    }

    @Pure
    @Impure
    static <E extends Enum<E>> int recordingSessionStateChangeLength(final @Owning E from, final @Owning E to, final String reason)
    {
        return stateTransitionStringLength(from, to) + (2 * SIZE_OF_LONG) + (SIZE_OF_INT + reason.length());
    }

    @Impure
    static <E extends Enum<E>> int encodeReplicationSessionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final @Owning E from,
        final @Owning E to,
        final long replicationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final String reason)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, replicationId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, srcRecordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, dstRecordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;
        encodingBuffer.putLong(offset + encodedLength, position, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodedLength += encodeStateChange(encodingBuffer, offset + encodedLength, from, to);

        encodedLength += encodeTrailingString(encodingBuffer, offset + encodedLength,
            captureLength + LOG_HEADER_LENGTH - encodedLength, reason);

        return encodedLength;
    }

    @Pure
    @Impure
    static <E extends Enum<E>> int replicationSessionStateChangeLength(final @Owning E from, final @Owning E to, final String reason)
    {
        return stateTransitionStringLength(from, to) + (4 * SIZE_OF_LONG) + (SIZE_OF_INT + reason.length());
    }

    @Impure
    static <E extends Enum<E>> int encodeControlSessionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final @Owning E from,
        final @Owning E to,
        final long id,
        final String reason)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, id, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodedLength += encodeStateChange(encodingBuffer, offset + encodedLength, from, to);

        encodedLength += encodeTrailingString(encodingBuffer, offset + encodedLength,
            captureLength + LOG_HEADER_LENGTH - encodedLength, reason);

        return encodedLength;
    }

    @Pure
    @Impure
    static <E extends Enum<E>> int sessionStateChangeLength(final @Owning E from, final @Owning E to, final String reason)
    {
        return stateTransitionStringLength(from, to) + SIZE_OF_LONG + (SIZE_OF_INT + reason.length());
    }

    @Impure
    static void encodeReplaySessionError(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long sessionId,
        final long recordingId,
        final String errorMessage)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, sessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, recordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodeTrailingString(encodingBuffer, offset + encodedLength, captureLength - (SIZE_OF_INT * 2), errorMessage);
    }

    @Impure
    static void encodeCatalogResize(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long catalogLength,
        final long newCatalogLength)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, catalogLength, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, newCatalogLength, LITTLE_ENDIAN);
    }

    @Pure
    static int replicationSessionDoneLength()
    {
        return 8 * SIZE_OF_LONG + 3 * SIZE_OF_BYTE;
    }

    @Impure
    static void encodeReplicationSessionDone(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long controlSessionId,
        final long replicationId,
        final long srcRecordingId,
        final long replayPosition,
        final long srcStopPosition,
        final long dstRecordingId,
        final long dstStopPosition,
        final long position,
        final boolean isClosed,
        final boolean isEndOfStream,
        final boolean isSynced)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, controlSessionId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, replicationId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, srcRecordingId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, replayPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, srcStopPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, dstRecordingId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, dstStopPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putLong(bodyOffset + bodyLength, position, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;
        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)(isClosed ? 1 : 0));
        bodyLength += SIZE_OF_BYTE;
        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)(isEndOfStream ? 1 : 0));
        bodyLength += SIZE_OF_BYTE;
        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)(isSynced ? 1 : 0));
    }
}
