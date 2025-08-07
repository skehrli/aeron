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
package io.aeron.cluster.client;

import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Interface for consuming messages coming from the cluster that also include administrative events.
 */
@FunctionalInterface
public interface EgressListener
{
    /**
     * Message returned from the clustered service.
     *
     * @param clusterSessionId to which the message belongs.
     * @param timestamp        at which the correlated ingress was sequenced in the cluster.
     * @param buffer           containing the message.
     * @param offset           at which the message begins.
     * @param length           of the message in bytes.
     * @param header           Aeron header associated with the message fragment.
     */
    @SideEffectFree
    void onMessage(
        long clusterSessionId,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * Session event emitted from the cluster which after connect can indicate an error or session close.
     *
     * @param correlationId    associated with the cluster ingress.
     * @param clusterSessionId to which the event belongs.
     * @param leadershipTermId for identifying the active term of leadership
     * @param leaderMemberId   identity of the active leader.
     * @param code             to indicate the type of event.
     * @param detail           Textual detail to explain the event.
     */
    @SideEffectFree
    default void onSessionEvent(
        long correlationId,
        long clusterSessionId,
        long leadershipTermId,
        int leaderMemberId,
        EventCode code,
        String detail)
    {
    }

    /**
     * Event indicating a new leader has been elected.
     *
     * @param clusterSessionId to which the event belongs.
     * @param leadershipTermId for identifying the active term of leadership
     * @param leaderMemberId   identity of the active leader.
     * @param ingressEndpoints for connecting to the cluster which can be updated due to dynamic membership.
     */
    @SideEffectFree
    default void onNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, String ingressEndpoints)
    {
    }

    /**
     * Message returned in response to an admin request.
     *
     * @param clusterSessionId to which the response belongs.
     * @param correlationId    of the admin request.
     * @param requestType      of the admin request.
     * @param responseCode     describing the response.
     * @param message          describing the response (e.g. error message).
     * @param payload          delivered with the response, can be empty.
     * @param payloadOffset    into the payload buffer.
     * @param payloadLength    of the payload.
     */
    @SideEffectFree
    default void onAdminResponse(
        long clusterSessionId,
        long correlationId,
        AdminRequestType requestType,
        AdminResponseCode responseCode,
        String message,
        DirectBuffer payload,
        int payloadOffset,
        int payloadLength)
    {
    }
}
