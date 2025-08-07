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
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.CommonEventEncoder.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.*;

final class ClusterEventEncoder
{
    static final int MAX_REASON_LENGTH = 300;

    @SideEffectFree
    private ClusterEventEncoder()
    {
    }

    @Impure
    static int encodeOnNewLeadershipTerm(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderId,
        final int logSessionId,
        final int appVersion,
        final boolean isStartup)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, nextLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, nextTermBaseLogPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, nextLogPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, termBaseLogPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leaderRecordingId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, timestamp, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, leaderId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, logSessionId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, appVersion, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putByte(offset + encodedLength, (byte)(isStartup ? 1 : 0));
        encodedLength += SIZE_OF_BYTE;

        return encodedLength;
    }

    @Pure
    static int newLeaderShipTermLength()
    {
        return (SIZE_OF_LONG * 9) + (SIZE_OF_INT * 4) + SIZE_OF_BYTE;
    }

    @Impure
    static <E extends Enum<E>> int encodeStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final @Owning E from,
        final @Owning E to)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodeTrailingStateChange(encodingBuffer, offset, encodedLength, captureLength, from, to);
    }

    @Pure
    @Impure
    static <E extends Enum<E>> int stateChangeLength(final @Owning E from, final @Owning E to)
    {
        return stateTransitionStringLength(from, to) + SIZE_OF_INT;
    }

    @Impure
    static <E extends Enum<E>> int encodeElectionStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final @Owning E from,
        final @Owning E to,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition,
        final String reason)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, candidateTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, appendPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, catchupPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, leaderId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodedLength += CommonEventEncoder.encodeStateChange(encodingBuffer, offset + encodedLength, from, to);

        encodedLength += encodeTrailingString(
            encodingBuffer, offset + encodedLength, captureLength + LOG_HEADER_LENGTH - encodedLength, reason);

        return encodedLength;
    }

    @Pure
    @Impure
    static <E extends Enum<E>> int electionStateChangeLength(final @Owning E from, final @Owning E to, final String reason)
    {
        return (2 * SIZE_OF_INT) + (6 * SIZE_OF_LONG) + stateTransitionStringLength(from, to) +
            trailingStringLength(reason, MAX_REASON_LENGTH);
    }

    @Impure
    static int encodeOnCanvassPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, followerMemberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, protocolVersion, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodedLength;
    }

    @Pure
    static int canvassPositionLength()
    {
        return 3 * SIZE_OF_LONG + 3 * SIZE_OF_INT;
    }

    @Impure
    static int encodeOnRequestVote(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, candidateTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, candidateId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, protocolVersion, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        return encodedLength;
    }

    @Pure
    static int requestVoteLength()
    {
        return 3 * SIZE_OF_LONG + 3 * SIZE_OF_INT;
    }

    @Impure
    static int encodeOnCatchupPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, followerMemberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodeTrailingString(
            encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, catchupEndpoint);

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int catchupPositionLength(final String endpoint)
    {
        return 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT + SIZE_OF_INT + endpoint.length();
    }

    @Impure
    static int encodeOnStopCatchup(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int length,
        final int captureLength,
        final int memberId,
        final long leadershipTermId,
        final int followerMemberId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, followerMemberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        return logHeaderLength + bodyLength;
    }

    @Impure
    static <E extends Enum<E>> int encodeTruncateLogEntry(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int length,
        final int captureLength,
        final int memberId,
        final @Owning E state,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long candidateTermId,
        final long commitPosition,
        final long logPosition,
        final long appendPosition,
        final long oldPosition,
        final long newPosition)
    {
        int encodedLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + encodedLength, logLeadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, leadershipTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, candidateTermId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, commitPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, logPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, appendPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, oldPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + encodedLength, newPosition, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + encodedLength, memberId, LITTLE_ENDIAN);
        encodedLength += SIZE_OF_INT;

        encodedLength += encodingBuffer.putStringAscii(offset + encodedLength, enumName(state), LITTLE_ENDIAN);

        return encodedLength;
    }

    @Impure
    static int encodeOnReplayNewLeadershipTermEvent(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, timestamp, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, termBaseLogPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, appVersion, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)(isInElection ? 1 : 0));
        bodyLength += SIZE_OF_BYTE;

        final String unit = enumName(timeUnit);
        encodingBuffer.putStringAscii(bodyOffset + bodyLength, unit, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT + unit.length();

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int replayNewLeadershipTermEventLength(final TimeUnit timeUnit)
    {
        return (2 * SIZE_OF_INT) + (4 * SIZE_OF_LONG) + SIZE_OF_BYTE + SIZE_OF_INT + timeUnit.name().length();
    }

    @Impure
    static int encodeOnAppendPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, followerMemberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)flags);
        bodyLength += SIZE_OF_BYTE;

        return logHeaderLength + bodyLength;
    }

    @Impure
    static int encodeOnCommitPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int leaderId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, leaderId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int addPassiveMemberLength(final String endpoints)
    {
        return SIZE_OF_LONG + 2 * SIZE_OF_INT + endpoints.length();
    }

    @Impure
    static int encodeOnAddPassiveMember(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long correlationId,
        final String memberEndpoints)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, correlationId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodeTrailingString(
            encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, memberEndpoints);

        return logHeaderLength + bodyLength;
    }

    @Pure
    @Impure
    static int appendSessionCloseLength(final CloseReason closeReason, final TimeUnit timeUnit)
    {
        return 3 * SIZE_OF_LONG + SIZE_OF_INT + (SIZE_OF_INT + enumName(closeReason).length()) +
            (SIZE_OF_INT + enumName(timeUnit).length());
    }

    @Impure
    static int encodeAppendSessionClose(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long sessionId,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, sessionId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, timestamp, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodingBuffer.putStringAscii(bodyOffset + bodyLength, enumName(closeReason), LITTLE_ENDIAN);

        bodyLength += encodingBuffer.putStringAscii(bodyOffset + bodyLength, enumName(timeUnit), LITTLE_ENDIAN);

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int terminationPositionLength()
    {
        return SIZE_OF_INT + 2 * SIZE_OF_LONG;
    }

    @Impure
    static int encodeTerminationPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, logLeadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int terminationAckLength()
    {
        return 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT;
    }

    @Impure
    static int encodeTerminationAck(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final int senderMemberId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, logLeadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, senderMemberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        return logHeaderLength + bodyLength;
    }

    @Pure
    @Impure
    static int serviceAckLength(final TimeUnit timeUnit)
    {
        return (2 * SIZE_OF_INT) + (4 * SIZE_OF_LONG) + (SIZE_OF_INT + enumName(timeUnit).length());
    }

    @Impure
    public static int encodeServiceAck(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final long ackId,
        final long relevantId,
        final int serviceId)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, timestamp, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, ackId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, relevantId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, serviceId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodingBuffer.putStringAscii(bodyOffset + bodyLength, enumName(timeUnit), LITTLE_ENDIAN);

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int replicationEndedLength(final String purpose, final String controlUri)
    {
        return (3 * SIZE_OF_LONG) + (SIZE_OF_INT) + (SIZE_OF_BYTE) + (SIZE_OF_INT + purpose.length()) +
            (SIZE_OF_INT + controlUri.length());
    }

    @Impure
    static int encodeReplicationEnded(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final String purpose,
        final String channel,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, srcRecordingId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, dstRecordingId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, position, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putByte(bodyOffset + bodyLength, (byte)(hasSynced ? 1 : 0));
        bodyLength += SIZE_OF_BYTE;

        bodyLength += encodingBuffer.putStringAscii(bodyOffset + bodyLength, purpose, LITTLE_ENDIAN);
        bodyLength += encodeTrailingString(
            encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, channel);

        return logHeaderLength + bodyLength;
    }

    @Pure
    static int standbySnapshotNotificationLength(final TimeUnit timeUnit, final String archiveEndpoint)
    {
        return (5 * SIZE_OF_LONG) + (2 * SIZE_OF_LONG) +
            (2 * SIZE_OF_INT) + timeUnit.name().length() + archiveEndpoint.length();
    }

    @Impure
    static int encodeStandbySnapshotNotification(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final int serviceId,
        final String archiveEndpoint)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, recordingId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, termBaseLogPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, timestamp, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodingBuffer.putInt(bodyOffset + bodyLength, serviceId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        bodyLength += encodingBuffer.putStringAscii(bodyOffset + bodyLength, timeUnit.name(), LITTLE_ENDIAN);
        bodyLength += encodeTrailingString(
            encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, archiveEndpoint);

        return logHeaderLength + bodyLength;
    }

    @Pure
    @Impure
    static int newElectionLength(final String reason)
    {
        return (3 * SIZE_OF_LONG) + SIZE_OF_INT + trailingStringLength(reason, 300);
    }

    @Impure
    static int encodeNewElection(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final long appendPosition,
        final String reason)
    {
        final int logHeaderLength = encodeLogHeader(encodingBuffer, offset, captureLength, length);
        final int bodyOffset = offset + logHeaderLength;
        int bodyLength = 0;

        encodingBuffer.putLong(bodyOffset + bodyLength, leadershipTermId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, logPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putLong(bodyOffset + bodyLength, appendPosition, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_LONG;

        encodingBuffer.putInt(bodyOffset + bodyLength, memberId, LITTLE_ENDIAN);
        bodyLength += SIZE_OF_INT;

        encodeTrailingString(encodingBuffer, bodyOffset + bodyLength, captureLength - bodyLength, reason);

        return logHeaderLength + bodyLength;
    }
}
