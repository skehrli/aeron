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
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.ClusterEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static org.agrona.BitUtil.*;

/**
 * Event logger interface used by interceptors for recording cluster events into a {@link RingBuffer} for a
 * {@link ConsensusModule} events via a Java Agent.
 */
public final class ClusterEventLogger
{
    /**
     * Logger for writing into the {@link EventConfiguration#EVENT_RING_BUFFER}.
     */
    public static final ClusterEventLogger LOGGER = new ClusterEventLogger(EVENT_RING_BUFFER);

    private final ManyToOneRingBuffer ringBuffer;

    @SideEffectFree
    ClusterEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    /**
     * Log a new leadership term event.
     *
     * @param memberId                of the current cluster node.
     * @param logLeadershipTermId     term for which log entries are present.
     * @param nextLeadershipTermId    next term relative to the logLeadershipTermId
     * @param nextTermBaseLogPosition base log position for the next term.
     * @param nextLogPosition         committed log position for next term.
     * @param leadershipTermId        new leadership term id.
     * @param termBaseLogPosition     position the log reached at base of new term.
     * @param logPosition             position the log reached for the new term.
     * @param leaderRecordingId       of the log in the leader archive.
     * @param timestamp               of the new term.
     * @param leaderId                member id for the new leader.
     * @param logSessionId            session id of the log extension.
     * @param appVersion              associated with the recorded state.
     * @param isStartup               is the leader starting up fresh.
     */
    @Impure
    public void logOnNewLeadershipTerm(
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
        final int length = newLeaderShipTermLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(NEW_LEADERSHIP_TERM.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeOnNewLeadershipTerm(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    logLeadershipTermId,
                    nextLeadershipTermId,
                    nextTermBaseLogPosition,
                    nextLogPosition,
                    leadershipTermId,
                    termBaseLogPosition,
                    logPosition,
                    leaderRecordingId,
                    timestamp,
                    leaderId,
                    logSessionId,
                    appVersion,
                    isStartup);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a state change event for a cluster node.
     *
     * @param <E>       type representing the state change.
     * @param eventCode for the type of state change.
     * @param memberId  of the current cluster node.
     * @param oldState  before the change.
     * @param newState  after the change.
     */
    @Impure
    public <E extends Enum<E>> void logStateChange(
        final ClusterEventCode eventCode, final int memberId, final @Owning E oldState, final @Owning E newState)
    {
        final int length = stateChangeLength(oldState, newState);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(eventCode.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeStateChange(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    oldState,
                    newState
                );
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an election state change event for a cluster node.
     *
     * @param <E>                 type representing the state change.
     * @param memberId            on which the change has taken place.
     * @param oldState            before the change.
     * @param newState            after the change.
     * @param leaderId            of the cluster.
     * @param candidateTermId     of the node.
     * @param leadershipTermId    of the node.
     * @param logPosition         of the node.
     * @param logLeadershipTermId of the node.
     * @param appendPosition      of the node.
     * @param catchupPosition     of the node.
     * @param reason              for the state transition to occur.
     */
    @Impure
    public <E extends Enum<E>> void logElectionStateChange(
        final int memberId,
        final @Owning E oldState,
        final @Owning E newState,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition,
        final String reason)
    {
        final int length = electionStateChangeLength(oldState, newState, reason);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(ELECTION_STATE_CHANGE.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeElectionStateChange(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    oldState,
                    newState,
                    leaderId,
                    candidateTermId,
                    leadershipTermId,
                    logPosition,
                    logLeadershipTermId,
                    appendPosition,
                    catchupPosition,
                    reason);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a canvass position event received by the cluster node.
     *
     * @param memberId            member who sent the event.
     * @param logLeadershipTermId leadershipTermId reached by the member for it recorded log.
     * @param logPosition         position the member has durably recorded.
     * @param leadershipTermId    the most current leadershipTermId a member has seen.
     * @param followerMemberId    follower node id.
     * @param protocolVersion     of the consensus module.
     */
    @Impure
    public void logOnCanvassPosition(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
        final int length = canvassPositionLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(CANVASS_POSITION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeOnCanvassPosition(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    logLeadershipTermId,
                    logPosition,
                    leadershipTermId,
                    followerMemberId,
                    protocolVersion);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a request to vote from a cluster candidate for leadership.
     *
     * @param memberId            of the current cluster node.
     * @param logLeadershipTermId leadershipTermId processes from the log by the candidate.
     * @param logPosition         position reached in the log for the latest leadership term.
     * @param candidateTermId     the term id as the candidate sees it for the election.
     * @param candidateId         id of the candidate node.
     * @param protocolVersion     from the request.
     */
    @Impure
    public void logOnRequestVote(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
        final int length = requestVoteLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(REQUEST_VOTE.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeOnRequestVote(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    logLeadershipTermId,
                    logPosition,
                    candidateTermId,
                    candidateId,
                    protocolVersion);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the catchup position message.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId leadership term to catch up on
     * @param logPosition      position to catchup from
     * @param followerMemberId the id of the follower that is catching up
     * @param catchupEndpoint  the endpoint to send catchup messages
     */
    @Impure
    public void logOnCatchupPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final String catchupEndpoint)
    {
        final int length = catchupPositionLength(catchupEndpoint);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(CATCHUP_POSITION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeOnCatchupPosition(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    leadershipTermId,
                    logPosition,
                    followerMemberId,
                    catchupEndpoint);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the stop catchup message.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId current leadershipTermId.
     * @param followerMemberId id of follower currently catching up.
     */
    @Impure
    public void logOnStopCatchup(
        final int memberId, final long leadershipTermId, final int followerMemberId)
    {
        final int length = SIZE_OF_LONG + 2 * SIZE_OF_INT;
        final int encodedLength = encodedLength(length);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(STOP_CATCHUP.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeOnStopCatchup(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    memberId,
                    leadershipTermId,
                    followerMemberId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an event when a log entry is being truncated.
     *
     * @param <E>                 type of the enum.
     * @param memberId            the node which truncates its log entry.
     * @param state               of the election.
     * @param logLeadershipTermId the election is in.
     * @param leadershipTermId    the election is in.
     * @param candidateTermId     the election is in.
     * @param commitPosition      when the truncation happens.
     * @param logPosition         of the election.
     * @param appendPosition      of the election.
     * @param oldPosition         truncated from.
     * @param newPosition         truncated to.
     */
    @Impure
    public <E extends Enum<E>> void logOnTruncateLogEntry(
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
        final int length = SIZE_OF_INT + enumName(state).length() + SIZE_OF_INT + 8 * SIZE_OF_LONG;
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(TRUNCATE_LOG_ENTRY.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeTruncateLogEntry(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    state,
                    logLeadershipTermId,
                    leadershipTermId,
                    candidateTermId,
                    commitPosition,
                    logPosition,
                    appendPosition,
                    oldPosition,
                    newPosition);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the replay of the leadership term id.
     *
     * @param memberId            current memberId.
     * @param isInElection        an election is currently in process.
     * @param leadershipTermId    the logged leadership term id.
     * @param logPosition         current position in the log.
     * @param timestamp           logged timestamp.
     * @param termBaseLogPosition initial position for this term.
     * @param timeUnit            cluster time unit.
     * @param appVersion          version of the application.
     */
    @Impure
    public void logOnReplayNewLeadershipTermEvent(
        final int memberId,
        final boolean isInElection,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        final int length = replayNewLeadershipTermEventLength(timeUnit);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(REPLAY_NEW_LEADERSHIP_TERM.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeOnReplayNewLeadershipTermEvent(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    isInElection,
                    leadershipTermId,
                    logPosition,
                    timestamp,
                    termBaseLogPosition,
                    timeUnit,
                    appVersion);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * The Append position received by the leader from a follower.
     *
     * @param memberId         of the current cluster node.
     * @param leadershipTermId the current leadership term id.
     * @param logPosition      the current position in the log.
     * @param followerMemberId follower member sending the Append position.
     * @param flags            applied to append position by follower.
     */
    @Impure
    public void logOnAppendPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        final int length = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT + SIZE_OF_BYTE;
        final int encodedLength = encodedLength(length);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(APPEND_POSITION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeOnAppendPosition(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    memberId,
                    leadershipTermId,
                    logPosition,
                    followerMemberId,
                    flags);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * The commit position received by the follower form the leader.
     *
     * @param memberId         of the node receiving commit position message.
     * @param leadershipTermId the current leadership term id.
     * @param logPosition      the current position in the log.
     * @param leaderId         leader member sending the commit position.
     */
    @Impure
    public void logOnCommitPosition(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final int leaderId)
    {
        final int length = 2 * SIZE_OF_LONG + 2 * SIZE_OF_INT;
        final int encodedLength = encodedLength(length);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(COMMIT_POSITION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeOnCommitPosition(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    memberId,
                    leadershipTermId,
                    logPosition,
                    leaderId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log addition of a passive member to the cluster.
     *
     * @param memberId        of the current cluster node.
     * @param correlationId   correlationId for responding to the addition of the passive member.
     * @param memberEndpoints the endpoints for the new member.
     */
    @Impure
    public void logOnAddPassiveMember(final int memberId, final long correlationId, final String memberEndpoints)
    {
        final int length = addPassiveMemberLength(memberEndpoints);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(ADD_PASSIVE_MEMBER.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeOnAddPassiveMember(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    length,
                    length,
                    memberId,
                    correlationId,
                    memberEndpoints);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the appending of a session close event to the log.
     *
     * @param memberId         member (leader) publishing the event.
     * @param sessionId        session id of the session be closed.
     * @param closeReason      reason to close the session.
     * @param leadershipTermId current leadership term id.
     * @param timestamp        the current timestamp.
     * @param timeUnit         units for the timestamp.
     */
    @Impure
    public void logAppendSessionClose(
        final int memberId,
        final long sessionId,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        final int length = appendSessionCloseLength(closeReason, timeUnit);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(APPEND_SESSION_CLOSE.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeAppendSessionClose(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    sessionId,
                    closeReason,
                    leadershipTermId,
                    timestamp,
                    timeUnit);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the receiving of a termination position event.
     *
     * @param memberId            that received the termination position.
     * @param logLeadershipTermId leadership term for the supplied position.
     * @param logPosition         position to terminate at.
     */
    @Impure
    public void logTerminationPosition(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition)
    {
        final int length = terminationPositionLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(TERMINATION_POSITION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeTerminationPosition(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    logLeadershipTermId,
                    logPosition);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the receiving of an acknowledgement to a termination position event.
     *
     * @param memberId            that received the termination ack.
     * @param logLeadershipTermId leadership term for the supplied position.
     * @param logPosition         position to terminate at.
     * @param senderMemberId      member sending the ack.
     */
    @Impure
    public void logTerminationAck(
        final int memberId,
        final long logLeadershipTermId,
        final long logPosition,
        final int senderMemberId)
    {
        final int length = ClusterEventEncoder.terminationAckLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(TERMINATION_ACK.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeTerminationAck(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    logLeadershipTermId,
                    logPosition,
                    senderMemberId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an ack received from a cluster service.
     *
     * @param memberId    memberId receiving the ack.
     * @param logPosition position in the log when the ack was sent.
     * @param timestamp   timestamp when the ack was sent.
     * @param timeUnit    time unit used for the timestamp.
     * @param ackId       id of the ack.
     * @param relevantId  associated id used in the ack, e.g. recordingId for snapshot acks.
     * @param serviceId   the id of the service that sent the ack.
     */
    @Impure
    public void logServiceAck(
        final int memberId,
        final long logPosition,
        final long timestamp,
        final TimeUnit timeUnit,
        final long ackId,
        final long relevantId,
        final int serviceId)
    {
        final int length = ClusterEventEncoder.serviceAckLength(timeUnit);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(SERVICE_ACK.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeServiceAck(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    logPosition,
                    timestamp,
                    timeUnit,
                    ackId,
                    relevantId,
                    serviceId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a replication end event.
     *
     * @param memberId       memberId running the replication.
     * @param purpose        the reason for the replication.
     * @param channel        the channel used to connect to the source archive.
     * @param srcRecordingId source recording id.
     * @param dstRecordingId destination recording id.
     * @param position       the position where the recording ended.
     * @param hasSynced      was the sync event been received for the replication.
     */
    @Impure
    public void logReplicationEnded(
        final int memberId,
        final String purpose,
        final String channel,
        final long srcRecordingId,
        final long dstRecordingId,
        final long position,
        final boolean hasSynced)
    {
        final int length = ClusterEventEncoder.replicationEndedLength(purpose, channel);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(REPLICATION_ENDED.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeReplicationEnded(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    purpose,
                    channel,
                    srcRecordingId,
                    dstRecordingId,
                    position,
                    hasSynced);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a standby snapshot notification.
     *
     * @param memberId            memberId receiving the notification.
     * @param recordingId         the recording id of the standby snapshot in the remote archive.
     * @param leadershipTermId    the leadershipTermId of the standby snapshot.
     * @param termBaseLogPosition the termBaseLogPosition of the standby snapshot.
     * @param logPosition         the position of the standby snapshot when it is taken.
     * @param timestamp           the cluster timestamp when the snapshot is taken.
     * @param timeUnit            the cluster time unit.
     * @param serviceId           the serviceId for the snapshot.
     * @param archiveEndpoint     the endpoint holding the standby snapshot.
     */
    @Impure
    public void logStandbySnapshotNotification(
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
        final int length = ClusterEventEncoder.standbySnapshotNotificationLength(timeUnit, archiveEndpoint);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(STANDBY_SNAPSHOT_NOTIFICATION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeStandbySnapshotNotification(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    recordingId,
                    leadershipTermId,
                    termBaseLogPosition,
                    logPosition,
                    timestamp,
                    timeUnit,
                    serviceId,
                    archiveEndpoint);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log the start of the new election.
     *
     * @param memberId         memberId which start the election.
     * @param leadershipTermId of the member.
     * @param logPosition      the log position.
     * @param appendPosition   the append position.
     * @param reason           for election to be started.
     */
    @Impure
    public void logNewElection(
        final int memberId,
        final long leadershipTermId,
        final long logPosition,
        final long appendPosition,
        final String reason)
    {
        final int length = ClusterEventEncoder.newElectionLength(reason);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(NEW_ELECTION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                ClusterEventEncoder.encodeNewElection(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    memberId,
                    leadershipTermId,
                    logPosition,
                    appendPosition,
                    reason);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }
}
