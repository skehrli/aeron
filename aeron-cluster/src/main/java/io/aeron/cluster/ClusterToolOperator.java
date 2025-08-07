/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.aeron.cluster;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.checker.mustcall.qual.Owning;
import static io.aeron.Aeron.NULL_VALUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardCopyOption.*;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.Strings.isEmpty;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterNodeControlProperties;
import io.aeron.cluster.service.ConsensusModuleProxy;

/**
 * Actions for an operator tool to control cluster.
 * This class is used by {@link ClusterTool} by Aeron but also allows to be used / extended by other tools.
 *
 * @see ClusterTool
 */
public class ClusterToolOperator
{
    /**
     * Successful action return value = exist status.
     */
    public static final int SUCCESS = 0;
    private static final int MARK_FILE_VERSION_WITH_CLUSTER_SERVICES_DIR = 1;
    private static final int FAILURE = -1;

    private final String toolChannel;
    private final int toolStreamId;
    private final long timeoutMs;

    /**
     * Constructor.
     *
     * @param toolChannel  channel to use.
     * @param toolStreamId stream id to use.
     * @param timeoutMs    timeout in milliseconds.
     */
    @SideEffectFree
    protected ClusterToolOperator(
        final String toolChannel,
        final int toolStreamId,
        final long timeoutMs)
    {
        this.toolChannel = toolChannel;
        this.toolStreamId = toolStreamId;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Print out the PID of the cluster process.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     * @return exit status.
     */
    @Impure
    protected int pid(final File clusterDir, final PrintStream out)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                out.println(markFile.decoder().pid());
            }
        }
        else
        {
            return FAILURE;
        }
        return SUCCESS;
    }

    /**
     * Print out the {@link io.aeron.cluster.RecordingLog.RecoveryPlan} for the cluster.
     *
     * @param out          to print the output to.
     * @param clusterDir   where the cluster is running.
     * @param serviceCount of services running in the containers.
     * @return exit status
     */
    @Impure
    protected int recoveryPlan(final PrintStream out, final File clusterDir, final int serviceCount)
    {
        try (AeronArchive archive = AeronArchive.connect();
            RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            out.println(recordingLog.createRecoveryPlan(archive, serviceCount, Aeron.NULL_VALUE));
        }
        return SUCCESS;
    }

    /**
     * Print out the {@link RecordingLog} for the cluster.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @return exit status
     */
    @Impure
    protected int recordingLog(final File clusterDir, final PrintStream out)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            out.println(recordingLog);
        }
        return SUCCESS;
    }

    /**
     * Re-order entries in thee {@link RecordingLog} file on disc if they are not in a proper order.
     *
     * @param clusterDir where the cluster is running.
     * @return {@code SUCCESS} if file contents was changed or {@code FAILURE} if it was already in the correct order.
     */
    @Impure
    protected int sortRecordingLog(final File clusterDir)
    {
        final List<RecordingLog.Entry> entries;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            entries = recordingLog.entries();
            if (isRecordingLogSorted(entries))
            {
                return FAILURE;
            }
        }
        catch (final RuntimeException ex)
        {
            return FAILURE;
        }

        updateRecordingLog(clusterDir, entries);

        return SUCCESS;
    }

    /**
     * Create a new {@link RecordingLog} based on the latest valid snapshot whose term base and log positions are set
     * to zero. The original recording log file is backed up as {@code recording.log.bak}.
     *
     * @param clusterDir where the cluster is running.
     * @return exit status
     */
    @Impure
    protected int seedRecordingLogFromSnapshot(final File clusterDir)
    {
        int snapshotIndex = Aeron.NULL_VALUE;
        final List<RecordingLog.Entry> entries;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            entries = recordingLog.entries();
            for (int i = entries.size() - 1; i >= 0; i--)
            {
                final RecordingLog.Entry entry = entries.get(i);
                if (RecordingLog.isValidSnapshot(entry) && ConsensusModule.Configuration.SERVICE_ID == entry.serviceId)
                {
                    snapshotIndex = i;
                    break;
                }
            }
        }

        final Path recordingLogBackup = clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME + ".bak");
        try
        {
            Files.copy(
                clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME),
                recordingLogBackup,
                REPLACE_EXISTING,
                COPY_ATTRIBUTES);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }

        if (Aeron.NULL_VALUE == snapshotIndex)
        {
            updateRecordingLog(clusterDir, Collections.emptyList());
        }
        else
        {
            final List<RecordingLog.Entry> truncatedEntries = new ArrayList<>();
            int serviceId = ConsensusModule.Configuration.SERVICE_ID;
            for (int i = snapshotIndex; i >= 0; i--)
            {
                final RecordingLog.Entry entry = entries.get(i);
                if (RecordingLog.isValidSnapshot(entry) && entry.serviceId == serviceId)
                {
                    truncatedEntries.add(new RecordingLog.Entry(
                        entry.recordingId,
                        entry.leadershipTermId,
                        0,
                        0,
                        entry.timestamp,
                        entry.serviceId,
                        entry.type,
                        null,
                        entry.isValid,
                        NULL_VALUE));
                    serviceId++;
                }
                else
                {
                    break;
                }
            }
            Collections.reverse(truncatedEntries);

            updateRecordingLog(clusterDir, truncatedEntries);
        }
        return SUCCESS;
    }

    /**
     * Print out the errors in the error logs for the cluster components.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @return exit status
     */
    @Impure
    protected int errors(final File clusterDir, final PrintStream out)
    {
        File clusterServicesDir = clusterDir;
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                printTypeAndActivityTimestamp(out, markFile);
                printErrors(out, markFile);

                final String aeronDirectory = markFile.decoder().aeronDirectory();
                out.println();
                printDriverErrors(out, aeronDirectory);

                clusterServicesDir = resolveClusterServicesDir(clusterDir, markFile.decoder());
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
        }

        final ClusterMarkFile[] serviceMarkFiles = openServiceMarkFiles(clusterServicesDir, out::println);
        for (final ClusterMarkFile serviceMarkFile : serviceMarkFiles)
        {
            printTypeAndActivityTimestamp(out, serviceMarkFile);
            printErrors(out, serviceMarkFile);
            serviceMarkFile.close();
        }
        return SUCCESS;
    }

    /**
     * Print out a list of the current members of the cluster.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @return exit status
     */
    @Impure
    protected int listMembers(final File clusterDir, final PrintStream out)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                final ClusterMembership clusterMembership = new ClusterMembership();
                final long timeoutMsToUse = Math.max(TimeUnit.SECONDS.toMillis(1), this.timeoutMs);

                if (queryClusterMembers(markFile, timeoutMsToUse, clusterMembership))
                {
                    out.println(
                        "currentTimeNs=" + clusterMembership.currentTimeNs +
                        ", leaderMemberId=" + clusterMembership.leaderMemberId +
                        ", memberId=" + clusterMembership.memberId +
                        ", activeMembers=" + clusterMembership.activeMembers +
                        ", passiveMembers=" + clusterMembership.passiveMembers);
                }
                else
                {
                    out.println("timeout waiting for response from node");
                    return FAILURE;
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return FAILURE;
        }
        return SUCCESS;
    }

    /**
     * Print out the time of the next backup query.
     *
     * @param clusterDir where the cluster backup is running.
     * @param out        to print the output to.
     * @return exit status
     */
    @Impure
    protected int printNextBackupQuery(final File clusterDir, final PrintStream out)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                if (markFile.decoder().componentType() != ClusterComponentType.BACKUP)
                {
                    out.println("not a cluster backup node");
                    return FAILURE;
                }
                else
                {
                    out.format("%2$tF %1$tH:%1$tM:%1$tS next: %2$tF %2$tH:%2$tM:%2$tS%n",
                        new Date(),
                        new Date(nextBackupQueryDeadlineMs(markFile)));
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return FAILURE;
        }
        return SUCCESS;
    }

    /**
     * Set the time of the next backup query for the cluster backup.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @param delayMs    from the current time for the next backup query.
     * @return exit status
     */
    @Impure
    protected int nextBackupQuery(final File clusterDir, final PrintStream out, final long delayMs)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                if (markFile.decoder().componentType() != ClusterComponentType.BACKUP)
                {
                    out.println("not a cluster backup node");
                    return FAILURE;
                }
                else
                {
                    final EpochClock epochClock = SystemEpochClock.INSTANCE;
                    nextBackupQueryDeadlineMs(markFile, epochClock.time() + delayMs);
                    out.format("%2$tF %1$tH:%1$tM:%1$tS setting next: %2$tF %2$tH:%2$tM:%2$tS%n",
                        new Date(),
                        new Date(nextBackupQueryDeadlineMs(markFile)));
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return FAILURE;
        }
        return SUCCESS;
    }

    /**
     * Print out the descriptors in the {@link ClusterMarkFile}s.
     *
     * @param out              to print the output to.
     * @param serviceMarkFiles to query.
     */
    @Impure
    protected void describe(final PrintStream out, final ClusterMarkFile[] serviceMarkFiles)
    {
        for (final ClusterMarkFile serviceMarkFile : serviceMarkFiles)
        {
            printTypeAndActivityTimestamp(out, serviceMarkFile);
            out.println(serviceMarkFile.decoder());
            serviceMarkFile.close();
        }
    }

    /**
     * Determine if the given node is a leader.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @return 0 if the node is an active leader in a closed election, 1 if not,
     * -1 if the mark file does not exist.
     */
    @Impure
    protected int isLeader(final File clusterDir, final PrintStream out)
    {
        if (markFileExists(clusterDir))
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                final String aeronDirectoryName = markFile.decoder().aeronDirectory();
                final MutableLong nodeRoleCounter = new MutableLong(-1);
                final MutableLong electionStateCounter = new MutableLong(-1);

                try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)))
                {
                    final CountersReader countersReader = aeron.countersReader();

                    countersReader.forEach(
                        (counterId, typeId, keyBuffer, label) ->
                        {
                            if (ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID == typeId)
                            {
                                nodeRoleCounter.set(countersReader.getCounterValue(counterId));
                            }
                            else if (ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID == typeId)
                            {
                                electionStateCounter.set(countersReader.getCounterValue(counterId));
                            }
                        });
                }

                if (nodeRoleCounter.get() == Cluster.Role.LEADER.code() &&
                    electionStateCounter.get() == ElectionState.CLOSED.code())
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return -1;
        }
    }

    /**
     * Does a {@link ClusterMarkFile} exist in the cluster directory.
     *
     * @param clusterDir to check for if a mark file exists.
     * @return true if the cluster mark file exists.
     */
    @SideEffectFree
    @Impure
    protected boolean markFileExists(final File clusterDir)
    {
        final File markFileDir = resolveClusterMarkFileDir(clusterDir);
        final File markFile = new File(markFileDir, ClusterMarkFile.FILENAME);

        return markFile.exists();
    }

    /**
     * List the current members of a cluster.
     *
     * @param clusterMembership to populate.
     * @param clusterDir        where the cluster is running.
     * @param timeoutMs         to wait on the query completing.
     * @return true if successful.
     */
    @Impure
    protected boolean listMembers(
        final ClusterMembership clusterMembership, final File clusterDir, final long timeoutMs)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                return queryClusterMembers(markFile, timeoutMs, clusterMembership);
            }
        }

        return false;
    }

    /**
     * Query the membership of a cluster.
     *
     * @param markFile          for the cluster component.
     * @param timeoutMs         to wait for the query.
     * @param clusterMembership to populate.
     * @return true if the query was successful.
     */
    @Impure
    protected boolean queryClusterMembers(
        final ClusterMarkFile markFile, final long timeoutMs, final ClusterMembership clusterMembership)
    {
        return queryClusterMembers(markFile.loadControlProperties(), timeoutMs, clusterMembership);
    }

    /**
     * Query the membership of a cluster.
     *
     * @param controlProperties from a {@link ClusterMarkFile}.
     * @param timeoutMs         to wait for the query.
     * @param clusterMembership to populate.
     * @return true if the query was successful.
     */
    @Impure
    protected boolean queryClusterMembers(
        final ClusterNodeControlProperties controlProperties,
        final long timeoutMs,
        final ClusterMembership clusterMembership)
    {
        final MutableLong id = new MutableLong(NULL_VALUE);
        final ClusterControlAdapter.Listener listener = new ClusterControlAdapter.Listener()
        {
            @Impure
            public void onClusterMembersResponse(
                final long correlationId,
                final int leaderMemberId,
                final String activeMembers,
                final String passiveMembers)
            {
                if (correlationId == id.get())
                {
                    clusterMembership.leaderMemberId = leaderMemberId;
                    clusterMembership.activeMembersStr = activeMembers;
                    clusterMembership.passiveMembersStr = passiveMembers;
                    id.set(NULL_VALUE);
                }
            }

            @Impure
            public void onClusterMembersExtendedResponse(
                final long correlationId,
                final long currentTimeNs,
                final int leaderMemberId,
                final int memberId,
                final List<ClusterMember> activeMembers,
                final List<ClusterMember> passiveMembers)
            {
                if (correlationId == id.get())
                {
                    clusterMembership.currentTimeNs = currentTimeNs;
                    clusterMembership.leaderMemberId = leaderMemberId;
                    clusterMembership.memberId = memberId;
                    clusterMembership.activeMembers = activeMembers;
                    clusterMembership.passiveMembers = passiveMembers;
                    clusterMembership.activeMembersStr = ClusterMember.encodeAsString(activeMembers);
                    clusterMembership.passiveMembersStr = ClusterMember.encodeAsString(passiveMembers);
                    id.set(NULL_VALUE);
                }
            }
        };

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(controlProperties.aeronDirectoryName));
            ConcurrentPublication publication = aeron.addPublication(
                controlProperties.controlChannel, controlProperties.consensusModuleStreamId);
            ConsensusModuleProxy consensusModuleProxy = new ConsensusModuleProxy(publication);
            ClusterControlAdapter clusterControlAdapter = new ClusterControlAdapter(aeron.addSubscription(
                controlProperties.controlChannel, controlProperties.serviceStreamId), listener))
        {
            final long deadlineMs = System.currentTimeMillis() + timeoutMs;
            final long correlationId = aeron.nextCorrelationId();
            id.set(correlationId);

            while (publication.availableWindow() <= 0)
            {
                if (System.currentTimeMillis() > deadlineMs)
                {
                    return false;
                }
                Thread.yield();
            }

            while (!consensusModuleProxy.clusterMembersQuery(correlationId))
            {
                if (System.currentTimeMillis() > deadlineMs)
                {
                    return false;
                }
                Thread.yield();
            }

            while (NULL_VALUE != id.get())
            {
                if (0 == clusterControlAdapter.poll())
                {
                    if (System.currentTimeMillis() > deadlineMs)
                    {
                        return false;
                    }
                    Thread.yield();
                }
            }

            return true;
        }
    }

    /**
     * Get the deadline time (MS) for the next cluster backup query.
     *
     * @param clusterDir where the cluster component is running.
     * @return the deadline time (MS) for the next cluster backup query, or {@link Aeron#NULL_VALUE} not available.
     */
    @Impure
    protected long nextBackupQueryDeadlineMs(final File clusterDir)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                return nextBackupQueryDeadlineMs(markFile);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the deadline time (MS) for the next cluster backup query.
     *
     * @param markFile for the cluster component.
     * @return the deadline time (MS) for the next cluster backup query, or {@link Aeron#NULL_VALUE} not available.
     */
    @Impure
    protected long nextBackupQueryDeadlineMs(final ClusterMarkFile markFile)
    {
        final String aeronDirectoryName = markFile.decoder().aeronDirectory();
        final MutableLong nextQueryMs = new MutableLong(NULL_VALUE);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)))
        {
            aeron.countersReader().forEach(
                (counterId, typeId, keyBuffer, label) ->
                {
                    if (ClusterBackup.QUERY_DEADLINE_TYPE_ID == typeId)
                    {
                        nextQueryMs.set(aeron.countersReader().getCounterValue(counterId));
                    }
                });
        }

        return nextQueryMs.get();
    }

    /**
     * Set the deadline time (MS) for the next cluster backup query.
     *
     * @param clusterDir where the cluster component is running.
     * @param timeMs     to set for the next deadline.
     * @return true if successful, otherwise false.
     */
    @Impure
    protected boolean nextBackupQueryDeadlineMs(final File clusterDir, final long timeMs)
    {
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                return nextBackupQueryDeadlineMs(markFile, timeMs);
            }
        }

        return false;
    }

    /**
     * Set the deadline time (MS) for the next cluster backup query.
     *
     * @param markFile for the cluster component.
     * @param timeMs   to set for the next deadline.
     * @return true if successful, otherwise false.
     */
    @Impure
    protected boolean nextBackupQueryDeadlineMs(final ClusterMarkFile markFile, final long timeMs)
    {
        final String aeronDirectoryName = markFile.decoder().aeronDirectory();
        final MutableBoolean result = new MutableBoolean(false);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)))
        {
            final CountersReader countersReader = aeron.countersReader();

            countersReader.forEach(
                (counterId, typeId, keyBuffer, label) ->
                {
                    if (ClusterBackup.QUERY_DEADLINE_TYPE_ID == typeId)
                    {
                        final AtomicCounter atomicCounter = new AtomicCounter(
                            countersReader.valuesBuffer(), counterId, null);

                        atomicCounter.setRelease(timeMs);
                        result.value = true;
                    }
                });
        }

        return result.value;
    }

    /**
     * Invalidate the latest snapshot so recovery will use an earlier one or log if no earlier one exists.
     *
     * @param clusterDir where the cluster component is running.
     * @param out        to print the operation result.
     * @return SUCCESS if the latest snapshot was invalidated, else FAILURE
     */
    @Impure
    protected int invalidateLatestSnapshot(final File clusterDir, final PrintStream out)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            final boolean result = recordingLog.invalidateLatestSnapshot();
            out.println(" invalidate latest snapshot: " + result);
            return result ? SUCCESS : FAILURE;
        }
    }

    /**
     * Print out a summary of the state captured in the latest consensus module snapshot.
     *
     * @param clusterDir where the cluster is running.
     * @param out        where to print the operation result.
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int describeLatestConsensusModuleSnapshot(
        final File clusterDir,
        final PrintStream out)
    {
        return describeLatestConsensusModuleSnapshot(
            clusterDir,
            out,
            null);
    }

    /**
     * Print out a summary of the state captured in the latest consensus module snapshot.
     *
     * @param clusterDir                  where the cluster is running.
     * @param out                         where to print the operation result.
     * @param postConsensusImageDescriber describing of image after consensus module state
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int describeLatestConsensusModuleSnapshot(
        final File clusterDir,
        final PrintStream out,
        final BiConsumer<Image, Aeron> postConsensusImageDescriber)
    {
        final RecordingLog.Entry entry = findLatestValidSnapshot(clusterDir);
        if (null == entry)
        {
            out.println("Snapshot not found");
            return FAILURE;
        }

        final ClusterNodeControlProperties properties = loadControlProperties(clusterDir);

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc");

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(properties.aeronDirectoryName));
            AeronArchive archive = AeronArchive.connect(archiveCtx.aeron(aeron)))
        {
            final int sessionId = (int)archive.startReplay(
                entry.recordingId, 0, AeronArchive.NULL_LENGTH, toolChannel, toolStreamId);

            final String replayChannel = ChannelUri.addSessionId(toolChannel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replayChannel, toolStreamId))
            {
                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    archive.checkForErrorResponse();
                    Thread.yield();
                }

                final ConsensusModuleSnapshotAdapter adapter = new ConsensusModuleSnapshotAdapter(
                    image, new ConsensusModuleSnapshotPrinter(out));

                while (true)
                {
                    final int fragments = adapter.poll();
                    if (adapter.isDone())
                    {
                        break;
                    }

                    if (0 == fragments)
                    {
                        if (image.isClosed())
                        {
                            throw new ClusterException("snapshot ended unexpectedly: " + image);
                        }

                        archive.checkForErrorResponse();
                        Thread.yield();
                    }
                }

                out.println("Consensus Module Snapshot End:" +
                    " memberId=" + properties.memberId +
                    " recordingId=" + entry.recordingId +
                    " length=" + image.position());

                if (null != postConsensusImageDescriber)
                {
                    postConsensusImageDescriber.accept(image, aeron);
                }
            }
        }
        return SUCCESS;
    }

    /**
     * Instruct the cluster to take a snapshot.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int snapshot(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.SNAPSHOT,
            true,
            TimeUnit.SECONDS.toMillis(30)) ? SUCCESS : FAILURE;
    }

    /**
     * Instruct the cluster to suspend operation.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int suspend(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.SUSPEND,
            false,
            TimeUnit.SECONDS.toMillis(1)) ? SUCCESS : FAILURE;
    }

    /**
     * Instruct the cluster to resume operation.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int resume(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.SUSPENDED,
            ClusterControl.ToggleState.RESUME,
            true,
            TimeUnit.SECONDS.toMillis(1)) ? SUCCESS : FAILURE;
    }

    /**
     * Instruct the cluster to shut down.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int shutdown(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.SHUTDOWN,
            false,
            TimeUnit.SECONDS.toMillis(1)) ? SUCCESS : FAILURE;
    }

    /**
     * Instruct the cluster to abort.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return SUCCESS is the operation was successfully requested, else FAILURE
     */
    @Impure
    protected int abort(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.ABORT,
            false,
            TimeUnit.SECONDS.toMillis(1)) ? SUCCESS : FAILURE;
    }

    /**
     * Finds the latest valid snapshot from the log file.
     *
     * @param clusterDir where the cluster node is running.
     * @return entry or {@code null} if not found.
     */
    @Impure
    protected RecordingLog.Entry findLatestValidSnapshot(final File clusterDir)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            return recordingLog.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID);
        }
    }

    /**
     * Load {@link ClusterNodeControlProperties} from the mark file.
     *
     * @param clusterDir where the cluster node is running.
     * @return control properties.
     */
    @Impure
    protected ClusterNodeControlProperties loadControlProperties(final File clusterDir)
    {
        final ClusterNodeControlProperties properties;
        try (ClusterMarkFile markFile = openMarkFile(clusterDir))
        {
            properties = markFile.loadControlProperties();
        }
        return properties;
    }

    @Impure
    @SuppressWarnings("MethodLength")
    boolean toggleClusterState(
        final PrintStream out,
        final File clusterDir,
        final ConsensusModule.State expectedState,
        final ClusterControl.ToggleState toggleState,
        final boolean waitForToggleToComplete,
        final long defaultTimeoutMs)
    {
        return toggleState(
            out,
            clusterDir,
            true,
            expectedState,
            toggleState,
            ToggleApplication.CLUSTER_CONTROL,
            waitForToggleToComplete,
            defaultTimeoutMs);
    }

    @Impure
    @SuppressWarnings("MethodLength")
    <T extends Enum<T>> boolean toggleState(
        final PrintStream out,
        final File clusterDir,
        final boolean isLeaderRequired,
        final ConsensusModule.State expectedState,
        final @Owning T targetState,
        final ToggleApplication<T> toggleApplication,
        final boolean waitForToggleToComplete,
        final long defaultTimeoutMs)
    {
        if (!markFileExists(clusterDir) && timeoutMs <= 0)
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return false;
        }

        final int clusterId;
        final ClusterNodeControlProperties clusterNodeControlProperties;
        try (ClusterMarkFile markFile = openMarkFile(clusterDir))
        {
            clusterId = markFile.clusterId();
            clusterNodeControlProperties = markFile.loadControlProperties();
        }

        final ClusterMembership clusterMembership = new ClusterMembership();
        final long queryTimeoutMs = Math.max(TimeUnit.SECONDS.toMillis(1), timeoutMs);

        if (!queryClusterMembers(clusterNodeControlProperties, queryTimeoutMs, clusterMembership))
        {
            out.println("Timed out querying cluster.");
            return false;
        }

        final String prefix = "Member [" + clusterMembership.memberId + "]: ";

        if (isLeaderRequired && clusterMembership.leaderMemberId != clusterMembership.memberId)
        {
            out.println(prefix + "Current node is not the leader (leaderMemberId = " +
                clusterMembership.leaderMemberId + "), unable to " + targetState);
            return false;
        }

        final File cncFile = new File(clusterNodeControlProperties.aeronDirectoryName, CncFileDescriptor.CNC_FILE);
        if (!cncFile.exists())
        {
            out.println(prefix + "Unable to locate media driver. C`n`C file [" + cncFile.getAbsolutePath() +
                "] does not exist.");
            return false;
        }

        final CountersReader countersReader = ClusterControl.mapCounters(cncFile);
        try
        {
            final ConsensusModule.State moduleState = ConsensusModule.State.find(countersReader, clusterId);
            if (null == moduleState)
            {
                out.println(prefix + "Unable to resolve state of consensus module.");
                return false;
            }

            if (expectedState != moduleState)
            {
                out.println(prefix + "Unable to " + targetState + " as the state of the consensus module is " +
                    moduleState + ", but needs to be " + expectedState);
                return false;
            }

            final AtomicCounter controlToggle = toggleApplication.find(countersReader, clusterId);
            if (null == controlToggle)
            {
                out.println(prefix + "Failed to find control toggle");
                return false;
            }

            if (!toggleApplication.apply(controlToggle, targetState))
            {
                out.println(prefix + "Failed to apply " + targetState + ", current toggle value = " +
                    ClusterControl.ToggleState.get(controlToggle));
                return false;
            }

            if (waitForToggleToComplete)
            {
                final long toggleTimeoutMs = Math.max(defaultTimeoutMs, timeoutMs);
                final long deadlineMs = System.currentTimeMillis() + toggleTimeoutMs;
                T currentState = null;

                do
                {
                    Thread.yield();
                    if (System.currentTimeMillis() > deadlineMs)
                    {
                        break;
                    }

                    currentState = toggleApplication.get(controlToggle);
                }
                while (!toggleApplication.isNeutral(currentState));

                if (!toggleApplication.isNeutral(currentState))
                {
                    out.println(prefix + "Timed out after " + toggleTimeoutMs + "ms waiting for " +
                        targetState + " to complete.");
                }
            }

            out.println(prefix + targetState + " applied successfully");

            return true;

        }
        finally
        {
            IoUtil.unmap(countersReader.valuesBuffer().byteBuffer());
        }
    }

    /**
     * Print out the descriptors in the {@link ClusterMarkFile}s.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @return exit status
     */
    // Used by Premium Tooling.
    @Impure
    @SuppressWarnings("UnusedReturnValue")
    protected int describeClusterMarkFile(final File clusterDir, final PrintStream out)
    {
        final File clusterServicesDir;
        if (markFileExists(clusterDir) || timeoutMs > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                final MarkFileHeaderDecoder decoder = markFile.decoder();

                printTypeAndActivityTimestamp(out, markFile);
                out.println(decoder);

                clusterServicesDir = resolveClusterServicesDir(clusterDir, decoder);
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return FAILURE;
        }

        final ClusterMarkFile[] serviceMarkFiles = openServiceMarkFiles(clusterServicesDir, out::println);
        describe(out, serviceMarkFiles);

        return SUCCESS;
    }

    @Impure
    File resolveClusterServicesDir(final File clusterDir, final MarkFileHeaderDecoder decoder)
    {
        final File clusterServicesDir;

        if (MARK_FILE_VERSION_WITH_CLUSTER_SERVICES_DIR <= decoder.sbeSchemaVersion())
        {
            decoder.sbeRewind();
            decoder.skipAeronDirectory();
            decoder.skipControlChannel();
            decoder.skipIngressChannel();
            decoder.skipServiceName();
            decoder.skipAuthenticator();

            final String servicesClusterDir = decoder.servicesClusterDir();
            clusterServicesDir = isEmpty(servicesClusterDir) ? clusterDir : new File(servicesClusterDir);
        }
        else
        {
            clusterServicesDir = clusterDir;
        }

        return clusterServicesDir;
    }

    @Impure
    ClusterMarkFile openMarkFile(final File clusterDir)
    {
        final File markFileDir = resolveClusterMarkFileDir(clusterDir);
        return new ClusterMarkFile(
            markFileDir, ClusterMarkFile.FILENAME, System::currentTimeMillis, timeoutMs, null);
    }

    @Impure
    private ClusterMarkFile[] openServiceMarkFiles(final File clusterDir, final Consumer<String> logger)
    {
        File[] clusterMarkFileNames = clusterDir.listFiles((dir, name) ->
            name.startsWith(ClusterMarkFile.SERVICE_FILENAME_PREFIX) &&
            (name.endsWith(ClusterMarkFile.FILE_EXTENSION) ||
            name.endsWith(ClusterMarkFile.LINK_FILE_EXTENSION)));

        if (null == clusterMarkFileNames)
        {
            clusterMarkFileNames = new File[0];
        }

        final ArrayList<File> resolvedMarkFileNames = new ArrayList<>();
        resolveServiceMarkFileNames(clusterMarkFileNames, resolvedMarkFileNames);

        final ClusterMarkFile[] clusterMarkFiles = new ClusterMarkFile[clusterMarkFileNames.length];

        for (int i = 0, n = resolvedMarkFileNames.size(); i < n; i++)
        {
            final File resolvedMarkFile = resolvedMarkFileNames.get(i);
            clusterMarkFiles[i] = new ClusterMarkFile(
                resolvedMarkFile.getParentFile(),
                resolvedMarkFile.getName(),
                System::currentTimeMillis,
                timeoutMs,
                logger);
        }

        return clusterMarkFiles;
    }

    @Impure
    private void resolveServiceMarkFileNames(final File[] clusterMarkFiles, final ArrayList<File> resolvedFiles)
    {
        final HashSet<String> resolvedServices = new HashSet<>();

        for (final File clusterMarkFile : clusterMarkFiles)
        {
            final String filename = clusterMarkFile.getName();
            if (filename.endsWith(ClusterMarkFile.LINK_FILE_EXTENSION))
            {
                final String name = filename.substring(
                    0, filename.length() - ClusterMarkFile.LINK_FILE_EXTENSION.length());

                final File markFileDir = resolveDirectoryFromLinkFile(clusterMarkFile);
                resolvedFiles.add(new File(markFileDir, name + ClusterMarkFile.FILE_EXTENSION));
                resolvedServices.add(name);
            }
        }

        for (final File clusterMarkFile : clusterMarkFiles)
        {
            final String filename = clusterMarkFile.getName();
            if (filename.endsWith(ClusterMarkFile.FILE_EXTENSION))
            {
                final String name = filename.substring(
                    0, filename.length() - ClusterMarkFile.FILE_EXTENSION.length());

                if (!resolvedServices.contains(name))
                {
                    resolvedFiles.add(clusterMarkFile);
                    resolvedServices.add(name);
                }
            }
        }
    }

    @Impure
    void printTypeAndActivityTimestamp(final PrintStream out, final ClusterMarkFile markFile)
    {
        printTypeAndActivityTimestamp(
            out,
            markFile.decoder().componentType().toString(),
            markFile.decoder().startTimestamp(),
            markFile.activityTimestampVolatile());
    }

    @Impure
    void printTypeAndActivityTimestamp(
        final PrintStream out,
        final String clusterComponentType,
        final long startTimestampMs,
        final long activityTimestampMs)
    {
        out.print("Type: " + clusterComponentType + " ");
        out.format(
            "%1$tH:%1$tM:%1$tS (start: %2$tF %2$tH:%2$tM:%2$tS, activity: %3$tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(startTimestampMs),
            new Date(activityTimestampMs));
    }

    @Impure
    void printErrors(final PrintStream out, final Supplier<AtomicBuffer> errorBuffer, final String name)
    {
        out.println(name + " component error log:");
        CommonContext.printErrorLog(errorBuffer.get(), out);
    }

    @Impure
    void printDriverErrors(final PrintStream out, final String aeronDirectory)
    {
        out.println("Aeron driver error log (directory: " + aeronDirectory + "):");
        final File cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        MappedByteBuffer cncByteBuffer = null;
        try
        {
            cncByteBuffer = IoUtil.mapExistingFile(cncFile, FileChannel.MapMode.READ_ONLY, "cnc");
            final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
            final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

            CncFileDescriptor.checkVersion(cncVersion);
            CommonContext.printErrorLog(CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), out);
        }
        finally
        {
            if (null != cncByteBuffer)
            {
                IoUtil.unmap(cncByteBuffer);
            }
        }
    }

    @SideEffectFree
    @Impure
    File resolveClusterMarkFileDir(final File dir)
    {
        final File linkFile = new File(dir, ClusterMarkFile.LINK_FILENAME);
        return linkFile.exists() ? resolveDirectoryFromLinkFile(linkFile) : dir;
    }

    @SideEffectFree
    File resolveDirectoryFromLinkFile(final File linkFile)
    {
        final File markFileDir;

        try
        {
            final byte[] bytes = Files.readAllBytes(linkFile.toPath());
            final String markFileDirPath = new String(bytes, US_ASCII).trim();
            markFileDir = new File(markFileDirPath);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException("failed to read link file=" + linkFile, ex);
        }

        return markFileDir;
    }

    /**
     * @return channel to use.
     */
    @Pure
    protected String toolChannel()
    {
        return toolChannel;
    }

    /**
     * @return stream id to use.
     */
    @Pure
    protected int toolStreamId()
    {
        return toolStreamId;
    }

    /**
     * @return timeout in milliseconds to use.
     */
    @Pure
    protected long timeoutMs()
    {
        return timeoutMs;
    }

    @Impure
    private void printErrors(final PrintStream out, final ClusterMarkFile markFile)
    {
        printErrors(out, markFile::errorBuffer, "Cluster");
    }

    @Pure
    private boolean isRecordingLogSorted(final List<RecordingLog.Entry> entries)
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            if (entries.get(i).entryIndex != i)
            {
                return false;
            }
        }

        return true;
    }

    @Impure
    private void updateRecordingLog(final File clusterDir, final List<RecordingLog.Entry> entries)
    {
        final Path recordingLog = clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        try
        {
            if (entries.isEmpty())
            {
                Files.delete(recordingLog);
            }
            else
            {
                final Path newRecordingLog = clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME + ".tmp");
                Files.deleteIfExists(newRecordingLog);
                final ByteBuffer byteBuffer =
                    ByteBuffer.allocateDirect(RecordingLog.MAX_ENTRY_LENGTH).order(LITTLE_ENDIAN);
                final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
                try (FileChannel fileChannel = FileChannel.open(newRecordingLog, CREATE_NEW, WRITE))
                {
                    long position = 0;
                    for (final RecordingLog.Entry e : entries)
                    {
                        RecordingLog.writeEntryToBuffer(e, buffer);
                        byteBuffer.limit(e.length()).position(0);

                        if (e.length() != fileChannel.write(byteBuffer, position))
                        {
                            throw new ClusterException("failed to write recording");
                        }
                        position += e.length();
                    }
                }
                finally
                {
                    BufferUtil.free(byteBuffer);
                }

                Files.delete(recordingLog);
                Files.move(newRecordingLog, recordingLog);
            }
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }
}
