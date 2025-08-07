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

import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import net.bytebuddy.agent.builder.AgentBuilder;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.EnumSet;
import java.util.Map;

import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.ConfigOption.DISABLED_CLUSTER_EVENT_CODES;
import static io.aeron.agent.ConfigOption.ENABLED_CLUSTER_EVENT_CODES;
import static io.aeron.agent.EventConfiguration.parseEventCodes;
import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Implementation of a component logger for cluster log events.
 */
public class ClusterComponentLogger implements ComponentLogger
{
    static final EnumSet<ClusterEventCode> ENABLED_EVENTS = EnumSet.noneOf(ClusterEventCode.class);

    private static final Object2ObjectHashMap<String, EnumSet<ClusterEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(ClusterEventCode.class));
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    @Impure
    public int typeCode()
    {
        return EventCodeType.CLUSTER.getTypeCode();
    }

    /**
     * {@inheritDoc}
     */
    @SideEffectFree
    @Impure
    public void decode(
        final MutableDirectBuffer buffer, final int offset, final int eventCodeId, final StringBuilder builder)
    {
        get(eventCodeId).decode(buffer, offset, builder);
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public AgentBuilder addInstrumentation(final AgentBuilder agentBuilder, final Map<String, String> configOptions)
    {
        ENABLED_EVENTS.clear();
        ENABLED_EVENTS.addAll(getClusterEventCodes(configOptions.get(ENABLED_CLUSTER_EVENT_CODES)));
        ENABLED_EVENTS.removeAll(getClusterEventCodes(configOptions.get(DISABLED_CLUSTER_EVENT_CODES)));

        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ELECTION_STATE_CHANGE,
            "Election",
            ClusterInterceptor.ElectionStateChange.class,
            "logStateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            TRUNCATE_LOG_ENTRY,
            "Election",
            ClusterInterceptor.TruncateLogEntry.class,
            "onTruncateLogEntry");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REPLAY_NEW_LEADERSHIP_TERM,
            "ConsensusModuleAgent",
            ClusterInterceptor.ReplayNewLeadershipTerm.class,
            "logOnReplayNewLeadershipTermEvent");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            APPEND_POSITION,
            "ConsensusModuleAgent",
            ClusterInterceptor.AppendPosition.class,
            "logOnAppendPosition");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            COMMIT_POSITION,
            "ConsensusModuleAgent",
            ClusterInterceptor.CommitPosition.class,
            "logOnCommitPosition");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ADD_PASSIVE_MEMBER,
            "ConsensusModuleAgent",
            ClusterInterceptor.AddPassiveMember.class,
            "logOnAddPassiveMember");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            APPEND_SESSION_CLOSE,
            "LogPublisher",
            ClusterInterceptor.AppendSessionClose.class,
            "logAppendSessionClose");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CLUSTER_BACKUP_STATE_CHANGE,
            "ClusterBackupAgent",
            ClusterInterceptor.ClusterBackupStateChange.class,
            "logStateChange"
        );

        tempBuilder = addClusterConsensusModuleAgentInstrumentation(tempBuilder);

        return tempBuilder;
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void reset()
    {
        ENABLED_EVENTS.clear();
    }

    @Impure
    private static EnumSet<ClusterEventCode> getClusterEventCodes(final String enabledEventCodes)
    {
        return parseEventCodes(
            ClusterEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            ClusterEventCode::get,
            ClusterEventCode::valueOf);
    }


    @Impure
    private static AgentBuilder addClusterConsensusModuleAgentInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            STATE_CHANGE,
            "ConsensusModuleAgent",
            ClusterInterceptor.ConsensusModuleStateChange.class,
            "logStateChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            ROLE_CHANGE,
            "ConsensusModuleAgent",
            ClusterInterceptor.ConsensusModuleRoleChange.class,
            "logRoleChange");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NEW_LEADERSHIP_TERM,
            "ConsensusModuleAgent",
            ClusterInterceptor.NewLeadershipTerm.class,
            "logOnNewLeadershipTerm");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CANVASS_POSITION,
            "ConsensusModuleAgent",
            ClusterInterceptor.CanvassPosition.class,
            "logOnCanvassPosition");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REQUEST_VOTE,
            "ConsensusModuleAgent",
            ClusterInterceptor.RequestVote.class,
            "logOnRequestVote");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            CATCHUP_POSITION,
            "ConsensusModuleAgent",
            ClusterInterceptor.CatchupPosition.class,
            "logOnCatchupPosition");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            STOP_CATCHUP,
            "ConsensusModuleAgent",
            ClusterInterceptor.StopCatchup.class,
            "logOnStopCatchup");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            TERMINATION_POSITION,
            "ConsensusModuleAgent",
            ClusterInterceptor.TerminationPosition.class,
            "logOnTerminationPosition");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            TERMINATION_ACK,
            "ConsensusModuleAgent",
            ClusterInterceptor.TerminationAck.class,
            "logOnTerminationAck");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            SERVICE_ACK,
            "ConsensusModuleAgent",
            ClusterInterceptor.ServiceAck.class,
            "logOnServiceAck");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            REPLICATION_ENDED,
            "ConsensusModuleAgent",
            ClusterInterceptor.ReplicationEnded.class,
            "logReplicationEnded");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            STANDBY_SNAPSHOT_NOTIFICATION,
            "ConsensusModuleAgent",
            ClusterInterceptor.StandbySnapshotNotification.class,
            "logStandbySnapshotNotification");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NEW_ELECTION,
            "ConsensusModuleAgent",
            ClusterInterceptor.NewElection.class,
            "logNewElection");

        return tempBuilder;
    }

    @Impure
    private static AgentBuilder addEventInstrumentation(
        final AgentBuilder agentBuilder,
        final ClusterEventCode code,
        final String typeName,
        final Class<?> interceptorClass,
        final String interceptorMethod)
    {
        if (!ENABLED_EVENTS.contains(code))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith(typeName))
            .transform((builder, typeDescription, classLoader, javaModule, protectionDomain) ->
                builder.visit(to(interceptorClass).on(named(interceptorMethod))));
    }
}
