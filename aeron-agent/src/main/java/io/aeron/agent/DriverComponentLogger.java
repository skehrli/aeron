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

import static io.aeron.agent.ConfigOption.DISABLED_DRIVER_EVENT_CODES;
import static io.aeron.agent.ConfigOption.ENABLED_DRIVER_EVENT_CODES;
import static io.aeron.agent.DriverEventCode.*;
import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Implementation of a component logger for media driver log events.
 */
public class DriverComponentLogger implements ComponentLogger
{
    static final EnumSet<DriverEventCode> ENABLED_EVENTS = EnumSet.noneOf(DriverEventCode.class);

    private static final Object2ObjectHashMap<String, EnumSet<DriverEventCode>> SPECIAL_EVENTS =
        new Object2ObjectHashMap<>();

    static
    {
        SPECIAL_EVENTS.put("all", EnumSet.allOf(DriverEventCode.class));
        SPECIAL_EVENTS.put("admin", EnumSet.complementOf(EnumSet.of(FRAME_IN, FRAME_OUT)));
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    @Impure
    public int typeCode()
    {
        return EventCodeType.DRIVER.getTypeCode();
    }

    /**
     * {@inheritDoc}
     */
    @SideEffectFree
    @Impure
    public void decode(
        final MutableDirectBuffer buffer, final int offset, final int eventCodeId, final StringBuilder builder)
    {
        DriverEventCode.get(eventCodeId).decode(buffer, offset, builder);
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public AgentBuilder addInstrumentation(final AgentBuilder agentBuilder, final Map<String, String> configOptions)
    {
        ENABLED_EVENTS.clear();
        ENABLED_EVENTS.addAll(getDriverEventCodes(configOptions.get(ENABLED_DRIVER_EVENT_CODES)));
        ENABLED_EVENTS.removeAll(getDriverEventCodes(configOptions.get(DISABLED_DRIVER_EVENT_CODES)));

        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addDriverConductorInstrumentation(tempBuilder);
        tempBuilder = addDriverCommandInstrumentation(tempBuilder);
        tempBuilder = addDriverSenderProxyInstrumentation(tempBuilder);
        tempBuilder = addDriverReceiverProxyInstrumentation(tempBuilder);
        tempBuilder = addDriverUdpChannelTransportInstrumentation(tempBuilder);
        tempBuilder = addChannelEndpointInstrumentation(tempBuilder);

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            UNTETHERED_SUBSCRIPTION_STATE_CHANGE,
            "UntetheredSubscription",
            DriverInterceptor.UntetheredSubscriptionStateChange.class,
            "logStateChange");

        tempBuilder = addDriverNameResolutionInstrumentation(tempBuilder);

        tempBuilder = addDriverFlowControlInstrumentation(tempBuilder);

        tempBuilder = addPublicationRevokeInstrumentation(tempBuilder);

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
    private static EnumSet<DriverEventCode> getDriverEventCodes(final String enabledEventCodes)
    {
        return EventConfiguration.parseEventCodes(
            DriverEventCode.class,
            enabledEventCodes,
            SPECIAL_EVENTS,
            DriverEventCode::get,
            DriverEventCode::get);
    }

    @Impure
    private static AgentBuilder addDriverConductorInstrumentation(final AgentBuilder agentBuilder)
    {
        final boolean hasImageHook = ENABLED_EVENTS.contains(REMOVE_IMAGE_CLEANUP);
        final boolean hasPublicationHook = ENABLED_EVENTS.contains(REMOVE_PUBLICATION_CLEANUP);
        final boolean hasSubscriptionHook = ENABLED_EVENTS.contains(REMOVE_SUBSCRIPTION_CLEANUP);

        if (!hasImageHook && !hasPublicationHook && !hasSubscriptionHook)
        {
            return agentBuilder;
        }

        return agentBuilder.type(nameEndsWith("DriverConductor"))
            .transform((builder, typeDescription, classLoader, javaModule, protectionDomain) ->
            {
                if (hasImageHook)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupImage.class)
                        .on(named("cleanupImage")));
                }
                if (hasPublicationHook)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupPublication.class)
                            .on(named("cleanupPublication")))
                        .visit(to(CleanupInterceptor.CleanupIpcPublication.class)
                            .on(named("cleanupIpcPublication")));
                }
                if (hasSubscriptionHook)
                {
                    builder = builder.visit(to(CleanupInterceptor.CleanupSubscriptionLink.class)
                        .on(named("cleanupSubscriptionLink")));
                }

                return builder;
            });
    }

    @Impure
    private static AgentBuilder addDriverCommandInstrumentation(final AgentBuilder agentBuilder)
    {
        if (CmdInterceptor.EVENTS.stream().noneMatch(ENABLED_EVENTS::contains))
        {
            return agentBuilder;
        }

        return agentBuilder
            .type(nameEndsWith("ClientCommandAdapter"))
            .transform((builder, typeDescription, classLoader, javaModule, protectionDomain) -> builder
                .visit(to(CmdInterceptor.class)
                    .on(named("onMessage"))))
            .type(nameEndsWith("ClientProxy"))
            .transform((builder, typeDescription, classLoader, javaModule, protectionDomain) -> builder
                .visit(to(CmdInterceptor.class)
                    .on(named("transmit"))));
    }

    @Impure
    private static AgentBuilder addDriverSenderProxyInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            SEND_CHANNEL_CREATION,
            "SenderProxy",
            ChannelEndpointInterceptor.SenderProxy.RegisterSendChannelEndpoint.class,
            "registerSendChannelEndpoint");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            SEND_CHANNEL_CLOSE,
            "SenderProxy",
            ChannelEndpointInterceptor.SenderProxy.CloseSendChannelEndpoint.class,
            "closeSendChannelEndpoint");

        return tempBuilder;
    }

    @Impure
    private static AgentBuilder addDriverReceiverProxyInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            RECEIVE_CHANNEL_CREATION,
            "ReceiverProxy",
            ChannelEndpointInterceptor.ReceiverProxy.RegisterReceiveChannelEndpoint.class,
            "registerReceiveChannelEndpoint");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            RECEIVE_CHANNEL_CLOSE,
            "ReceiverProxy",
            ChannelEndpointInterceptor.ReceiverProxy.CloseReceiveChannelEndpoint.class,
            "closeReceiveChannelEndpoint");

        return tempBuilder;
    }

    @Impure
    private static AgentBuilder addDriverUdpChannelTransportInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            FRAME_OUT,
            "UdpChannelTransport",
            ChannelEndpointInterceptor.UdpChannelTransport.SendHook.class,
            "sendHook");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            FRAME_IN,
            "UdpChannelTransport",
            ChannelEndpointInterceptor.UdpChannelTransport.ReceiveHook.class,
            "receiveHook");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            RESEND,
            "UdpChannelTransport",
            ChannelEndpointInterceptor.UdpChannelTransport.ResendHook.class,
            "resendHook");

        return tempBuilder;
    }

    @Impure
    private AgentBuilder addChannelEndpointInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAK_SENT,
            "ReceiveChannelEndpoint",
            ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.NakSent.class,
            "sendNakMessage");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAK_RECEIVED,
            "SendChannelEndpoint",
            ChannelEndpointInterceptor.SendChannelEndpointInterceptor.NakReceived.class,
            "onNakMessage");

        return tempBuilder;
    }


    @Impure
    private static AgentBuilder addDriverNameResolutionInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAME_RESOLUTION_NEIGHBOR_ADDED,
            "Neighbor",
            DriverInterceptor.NameResolution.NeighborAdded.class,
            "neighborAdded");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAME_RESOLUTION_NEIGHBOR_REMOVED,
            "Neighbor",
            DriverInterceptor.NameResolution.NeighborRemoved.class,
            "neighborRemoved");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAME_RESOLUTION_RESOLVE,
            "TimeTrackingNameResolver",
            DriverInterceptor.NameResolution.Resolve.class,
            "logResolve");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAME_RESOLUTION_LOOKUP,
            "TimeTrackingNameResolver",
            DriverInterceptor.NameResolution.Lookup.class,
            "logLookup");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            NAME_RESOLUTION_HOST_NAME,
            "TimeTrackingNameResolver",
            DriverInterceptor.NameResolution.HostName.class,
            "logHostName");

        return tempBuilder;
    }

    @Impure
    private static AgentBuilder addDriverFlowControlInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            FLOW_CONTROL_RECEIVER_ADDED,
            "AbstractMinMulticastFlowControl",
            DriverInterceptor.FlowControl.ReceiverAdded.class,
            "receiverAdded");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            FLOW_CONTROL_RECEIVER_REMOVED,
            "AbstractMinMulticastFlowControl",
            DriverInterceptor.FlowControl.ReceiverRemoved.class,
            "receiverRemoved");

        return tempBuilder;
    }

    @Impure
    private static AgentBuilder addPublicationRevokeInstrumentation(final AgentBuilder agentBuilder)
    {
        AgentBuilder tempBuilder = agentBuilder;
        tempBuilder = addEventInstrumentation(
            tempBuilder,
            PUBLICATION_REVOKE,
            "NetworkPublication",
            DriverInterceptor.Revoke.PublicationRevoke.class,
            "logRevoke");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            PUBLICATION_REVOKE,
            "IpcPublication",
            DriverInterceptor.Revoke.PublicationRevoke.class,
            "logRevoke");

        tempBuilder = addEventInstrumentation(
            tempBuilder,
            PUBLICATION_IMAGE_REVOKE,
            "PublicationImage",
            DriverInterceptor.Revoke.PublicationImageRevoke.class,
            "logRevoke");

        return tempBuilder;
    }

    @Impure
    private static AgentBuilder addEventInstrumentation(
        final AgentBuilder agentBuilder,
        final DriverEventCode code,
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
