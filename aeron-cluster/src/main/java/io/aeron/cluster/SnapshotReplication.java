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

import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.RecordingSignal;
import org.agrona.CloseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

class SnapshotReplication implements AutoCloseable
{
    private final ArrayList<RecordingLog.Snapshot> snapshotsPending = new ArrayList<>();
    private final MultipleRecordingReplication multipleRecordingReplication;

    @Impure
    SnapshotReplication(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel)
    {
        this(
            archive,
            srcControlStreamId,
            srcControlChannel,
            replicationChannel,
            TimeUnit.SECONDS.toNanos(10),
            TimeUnit.SECONDS.toNanos(1));
    }

    @SideEffectFree
    @Impure
    SnapshotReplication(
        final AeronArchive archive,
        final int srcControlStreamId,
        final String srcControlChannel,
        final String replicationChannel,
        final long replicationProgressTimeoutNs,
        final long replicationProgressIntervalNs)
    {
        multipleRecordingReplication = MultipleRecordingReplication.newInstance(
            archive,
            srcControlStreamId,
            srcControlChannel,
            replicationChannel,
            replicationProgressTimeoutNs,
            replicationProgressIntervalNs);
    }

    @Impure
    void addSnapshot(final RecordingLog.Snapshot snapshot)
    {
        snapshotsPending.add(snapshot);
        multipleRecordingReplication.addRecording(snapshot.recordingId, Aeron.NULL_VALUE, Aeron.NULL_VALUE);
    }

    @Impure
    int poll(final long nowNs)
    {
        return multipleRecordingReplication.poll(nowNs);
    }

    @Impure
    void onSignal(final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        multipleRecordingReplication.onSignal(correlationId, recordingId, position, signal);
    }

    @Pure
    @Impure
    boolean isComplete()
    {
        return multipleRecordingReplication.isComplete();
    }

    @Impure
    List<RecordingLog.Snapshot> snapshotsRetrieved()
    {
        final ArrayList<RecordingLog.Snapshot> snapshots = new ArrayList<>();
        for (int i = 0, n = snapshotsPending.size(); i < n; i++)
        {
            final RecordingLog.Snapshot pendingSnapshot = snapshotsPending.get(i);
            final long dstRecordingId = multipleRecordingReplication.completedDstRecordingId(
                pendingSnapshot.recordingId);
            snapshots.add(retrievedSnapshot(pendingSnapshot, dstRecordingId));
        }

        return snapshots;
    }

    @SideEffectFree
    @Impure
    static RecordingLog.Snapshot retrievedSnapshot(final RecordingLog.Snapshot pending, final long recordingId)
    {
        return new RecordingLog.Snapshot(
            recordingId,
            pending.leadershipTermId,
            pending.termBaseLogPosition,
            pending.logPosition,
            pending.timestamp,
            pending.serviceId);
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void close()
    {
        CloseHelper.close(multipleRecordingReplication);
    }
}
