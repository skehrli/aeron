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
import org.checkerframework.dataflow.qual.SideEffectFree;

class StandbySnapshotEntry
{
    private final long recordingId;
    private final long leadershipTermId;
    private final long termBaseLogPosition;
    private final long logPosition;
    private final long timestamp;
    private final int serviceId;
    private final String archiveEndpoint;

    @SideEffectFree
    StandbySnapshotEntry(
        final long recordingId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long timestamp,
        final int serviceId,
        final String archiveEndpoint)
    {
        this.recordingId = recordingId;
        this.leadershipTermId = leadershipTermId;
        this.termBaseLogPosition = termBaseLogPosition;
        this.logPosition = logPosition;
        this.timestamp = timestamp;
        this.serviceId = serviceId;
        this.archiveEndpoint = archiveEndpoint;
    }

    @Pure
    public long recordingId()
    {
        return recordingId;
    }

    @Pure
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    @Pure
    public long termBaseLogPosition()
    {
        return termBaseLogPosition;
    }

    @Pure
    public long logPosition()
    {
        return logPosition;
    }

    @Pure
    public long timestamp()
    {
        return timestamp;
    }

    @Pure
    public int serviceId()
    {
        return serviceId;
    }

    @Pure
    public String archiveEndpoint()
    {
        return archiveEndpoint;
    }
}
