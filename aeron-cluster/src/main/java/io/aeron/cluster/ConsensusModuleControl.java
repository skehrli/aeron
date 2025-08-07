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
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.security.AuthorisationService;

import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

/**
 * Control interface for performing operations on the consensus module from a {@link ConsensusModuleExtension}.
 */
public interface ConsensusModuleControl
{
    /**
     * The unique id for the hosting member of the cluster.
     *
     * @return unique id for the hosting member of the cluster.
     */
    @Pure
    int memberId();

    /**
     * Cluster time as {@link #timeUnit()}s since 1 Jan 1970 UTC.
     *
     * @return time as {@link #timeUnit()}s since 1 Jan 1970 UTC.
     * @see #timeUnit()
     */
    @Impure
    long time();

    /**
     * The unit of time applied when timestamping and invoking {@link #time()} operations.
     *
     * @return the unit of time applied when timestamping and invoking {@link #time()} operations.
     * @see #time()
     */
    @Pure
    TimeUnit timeUnit();

    /**
     * {@link IdleStrategy} which should be used by the extension when it experiences back-pressure or is undertaking
     * any long-running actions.
     *
     * @return the {@link IdleStrategy} which should be used by the extension when it experiences back-pressure or is
     * undertaking any long-running actions.
     */
    @Pure
    IdleStrategy idleStrategy();

    /**
     * The {@link ConsensusModule.Context} under which the extension is running.
     *
     * @return the {@link ConsensusModule.Context} under which the extension is running.
     */
    @Pure
    ConsensusModule.Context context();

    /**
     * The {@link Aeron} client to be used by the extension.
     *
     * @return the {@link Aeron} client to be used by the extension.
     */
    @Pure
    Aeron aeron();

    /**
     * The {@link AeronArchive} client to be used by the extension.
     *
     * @return the {@link AeronArchive} client to be used by the extension.
     */
    @Pure
    AeronArchive archive();

    /**
     * The {@link AuthorisationService} used by the consensus module.
     *
     * @return the {@link AuthorisationService} used by the consensus module.
     */
    @Pure
    AuthorisationService authorisationService();

    /**
     * Lookup a {@link ClusterClientSession} for a given id.
     *
     * @param clusterSessionId for the session to lookup.
     * @return a {@link ClusterClientSession} for a given id, otherwise {@code null} if not found.
     */
    @Impure
    ClusterClientSession getClientSession(long clusterSessionId);

    /**
     * Close a cluster session as an administrative function.
     *
     * @param clusterSessionId to be closed.
     */
    @Impure
    void closeClusterSession(long clusterSessionId);

    /**
     * Numeric id for the commit position counter.
     *
     * @return commit position counter id.
     */
    @Impure
    int commitPositionCounterId();

    /**
     * Numeric id for the cluster (used when running multiple clusters on the same media driver).
     *
     * @return numeric id for the cluster.
     * @see ConsensusModule.Context#clusterId(int)
     */
    @Impure
    int clusterId();

    /**
     * The current cluster member for this node.
     *
     * @return cluster member for this node.
     */
    @Pure
    ClusterMember clusterMember();
}
