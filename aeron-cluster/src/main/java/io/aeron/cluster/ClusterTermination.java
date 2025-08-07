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
package io.aeron.cluster;

import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;

class ClusterTermination
{
    private long deadlineNs;
    private boolean haveServicesTerminated;

    @SideEffectFree
    ClusterTermination(final long deadlineNs, final int serviceCount)
    {
        this.deadlineNs = deadlineNs;
        this.haveServicesTerminated = serviceCount <= 0;
    }

    @Impure
    void deadlineNs(final long deadlineNs)
    {
        this.deadlineNs = deadlineNs;
    }

    @Pure
    @Impure
    boolean canTerminate(final ClusterMember[] members, final long nowNs)
    {
        if (haveServicesTerminated)
        {
            boolean result = true;

            for (final ClusterMember member : members)
            {
                if (!member.isLeader() && !member.hasTerminated())
                {
                    result = false;
                    break;
                }
            }

            return result || nowNs >= deadlineNs;
        }

        return false;
    }

    @Impure
    void onServicesTerminated()
    {
        haveServicesTerminated = true;
    }

    @Impure
    void terminationPosition(
        final ErrorHandler errorHandler,
        final ConsensusPublisher consensusPublisher,
        final ClusterMember[] members,
        final ClusterMember thisMember,
        final long leadershipTermId,
        final long position)
    {
        for (final ClusterMember member : members)
        {
            member.hasTerminated(false);

            if (member != thisMember)
            {
                if (!consensusPublisher.terminationPosition(member.publication(), leadershipTermId, position))
                {
                    errorHandler.onError(new ClusterEvent(
                        "failed to send termination position to member=" + member.id(), AeronException.Category.WARN));
                }
            }
        }
    }
}
