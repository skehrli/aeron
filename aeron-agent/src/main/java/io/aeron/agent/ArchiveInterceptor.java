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
import net.bytebuddy.asm.Advice;

import static io.aeron.agent.ArchiveEventLogger.LOGGER;

/**
 * Intercepts calls in the archive which relate to state changes.
 */
class ArchiveInterceptor
{
    static class ReplaySessionStateChange
    {
        @Impure
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void logStateChange(
            final @Owning E oldState,
            final @Owning E newState,
            final long sessionId,
            final long recordingId,
            final long position,
            final String reason)
        {
            LOGGER.logReplaySessionStateChange(
                oldState, newState, sessionId, recordingId, position, reason);
        }
    }

    static class RecordingSessionStateChange
    {
        @Impure
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void logStateChange(
            final @Owning E oldState,
            final @Owning E newState,
            final long recordingId,
            final long position,
            final String reason)
        {
            LOGGER.logRecordingSessionStateChange(oldState, newState, recordingId, position, reason);
        }
    }

    static class ReplicationSessionStateChange
    {
        @Impure
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void logStateChange(
            final @Owning E oldState,
            final @Owning E newState,
            final long replicationId,
            final long srcRecordingId,
            final long dstRecordingId,
            final long position,
            final String reason)
        {
            LOGGER.logReplicationSessionStateChange(
                oldState, newState, replicationId, srcRecordingId, dstRecordingId, position, reason);
        }
    }

    static class ReplicationSessionDone
    {
        @Impure
        @Advice.OnMethodEnter
        static void logReplicationSessionDone(
            final long controlSessionId,
            final long replicationId,
            final long srcRecordingId,
            final long replayPosition,
            final long srcStopPosition,
            final long dstRecordingId,
            final long dstStopPosition,
            final long position,
            final boolean isClosed,
            final boolean isEndOfStream,
            final boolean isSynced)
        {
            LOGGER.logReplicationSessionDone(
                controlSessionId,
                replicationId,
                srcRecordingId,
                replayPosition,
                srcStopPosition,
                dstRecordingId,
                dstStopPosition,
                position,
                isClosed,
                isEndOfStream,
                isSynced);
        }
    }

    static class ControlSessionStateChange
    {
        @Impure
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void logStateChange(
            final @Owning E oldState, final @Owning E newState, final long controlSessionId, final String reason)
        {
            LOGGER.logControlSessionStateChange(oldState, newState, controlSessionId, reason);
        }
    }

    static class ReplaySession
    {
        @Impure
        @Advice.OnMethodEnter
        static void onPendingError(final long sessionId, final long recordingId, final String errorMessage)
        {
            LOGGER.logReplaySessionError(sessionId, recordingId, errorMessage);
        }
    }

    static class Catalog
    {
        @Impure
        @Advice.OnMethodEnter
        static void catalogResized(final long catalogLength, final long newCatalogLength)
        {
            LOGGER.logCatalogResize(catalogLength, newCatalogLength);
        }
    }
}
