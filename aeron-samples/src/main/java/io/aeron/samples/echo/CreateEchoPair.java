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
package io.aeron.samples.echo;
import org.checkerframework.dataflow.qual.Impure;

class CreateEchoPair extends ProvisioningMessage
{
    final long correlationId;
    final String subscriptionChannel;
    final int subscriptionStream;
    final String publicationChannel;
    final int publicationStream;

    @Impure
    CreateEchoPair(
        final long correlationId,
        final String subscriptionChannel,
        final int subscriptionStream,
        final String publicationChannel,
        final int publicationStream)
    {
        this.correlationId = correlationId;
        this.subscriptionChannel = subscriptionChannel;
        this.subscriptionStream = subscriptionStream;
        this.publicationChannel = publicationChannel;
        this.publicationStream = publicationStream;
    }
}
