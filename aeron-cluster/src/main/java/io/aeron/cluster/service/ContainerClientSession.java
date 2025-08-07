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
package io.aeron.cluster.service;

import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Pure;
import io.aeron.*;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.*;

import java.util.Arrays;

final class ContainerClientSession implements ClientSession
{
    private final long id;
    private final int responseStreamId;
    private final String responseChannel;
    private final byte[] encodedPrincipal;

    private final ClusteredServiceAgent clusteredServiceAgent;
    private Publication responsePublication;
    private boolean isClosing;

    @SideEffectFree
    ContainerClientSession(
        final long sessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal,
        final ClusteredServiceAgent clusteredServiceAgent)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
        this.encodedPrincipal = encodedPrincipal;
        this.clusteredServiceAgent = clusteredServiceAgent;
    }


    @Pure
    public long id()
    {
        return id;
    }

    @Pure
    public int responseStreamId()
    {
        return responseStreamId;
    }

    @Pure
    public String responseChannel()
    {
        return responseChannel;
    }

    @Pure
    public byte[] encodedPrincipal()
    {
        return encodedPrincipal;
    }

    @Impure
    public void close()
    {
        if (null != clusteredServiceAgent.getClientSession(id))
        {
            clusteredServiceAgent.closeClientSession(id);
        }
    }

    @Pure
    public boolean isClosing()
    {
        return isClosing;
    }

    @Impure
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return clusteredServiceAgent.offer(id, responsePublication, buffer, offset, length);
    }

    @Impure
    public long offer(final DirectBufferVector[] vectors)
    {
        return clusteredServiceAgent.offer(id, responsePublication, vectors);
    }

    @Impure
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        return clusteredServiceAgent.tryClaim(id, responsePublication, length, bufferClaim);
    }

    @Impure
    void connect(final Aeron aeron)
    {
        try
        {
            if (null == responsePublication)
            {
                responsePublication = aeron.addPublication(responseChannel, responseStreamId);
            }
        }
        catch (final RegistrationException ex)
        {
            clusteredServiceAgent.handleError(new ClusterException(
                "failed to connect session response publication: " + ex.getMessage(), AeronException.Category.WARN));
        }
    }

    @Impure
    void markClosing()
    {
        this.isClosing = true;
    }

    @Impure
    void resetClosing()
    {
        isClosing = false;
    }

    @Impure
    void disconnect(final ErrorHandler errorHandler)
    {
        CloseHelper.close(errorHandler, responsePublication);
        responsePublication = null;
    }

    /**
     * {@inheritDoc}
     */
    @SideEffectFree
    public String toString()
    {
        return "ClientSession{" +
            "id=" + id +
            ", responseStreamId=" + responseStreamId +
            ", responseChannel='" + responseChannel + '\'' +
            ", encodedPrincipal=" + Arrays.toString(encodedPrincipal) +
            ", responsePublication=" + responsePublication +
            ", isClosing=" + isClosing +
            '}';
    }
}
