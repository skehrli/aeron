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
import io.aeron.cluster.codecs.EventCode;
import io.aeron.security.SessionProxy;

import static io.aeron.cluster.ClusterSession.State.CHALLENGED;

/**
 * Proxy for a session being authenticated by an {@link io.aeron.security.Authenticator}.
 */
final class ClusterSessionProxy implements SessionProxy
{
    private final EgressPublisher egressPublisher;
    private ClusterSession clusterSession;

    @SideEffectFree
    ClusterSessionProxy(final EgressPublisher egressPublisher)
    {
        this.egressPublisher = egressPublisher;
    }

    @Impure
    SessionProxy session(final ClusterSession clusterSession)
    {
        this.clusterSession = clusterSession;
        return this;
    }

    @Pure
    @Impure
    public long sessionId()
    {
        return clusterSession.id();
    }

    @Impure
    public boolean challenge(final byte[] encodedChallenge)
    {
        if (egressPublisher.sendChallenge(clusterSession, encodedChallenge))
        {
            clusterSession.state(CHALLENGED);
            return true;
        }

        return false;
    }

    @Impure
    public boolean authenticate(final byte[] encodedPrincipal)
    {
        ClusterSession.checkEncodedPrincipalLength(encodedPrincipal);
        clusterSession.authenticate(encodedPrincipal);

        return true;
    }

    @Impure
    public void reject()
    {
        clusterSession.reject(EventCode.AUTHENTICATION_REJECTED, ConsensusModule.Configuration.SESSION_REJECTED_MSG);
    }
}
