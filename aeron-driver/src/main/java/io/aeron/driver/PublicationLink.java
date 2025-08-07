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
package io.aeron.driver;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;

/**
 * Tracks an Aeron client interest registration in a {@link NetworkPublication} or {@link IpcPublication}.
 */
final class PublicationLink implements DriverManagedResource
{
    private final long registrationId;
    private final Object publication;
    private final AeronClient client;
    private boolean reachedEndOfLife = false;

    @Impure
    PublicationLink(final long registrationId, final AeronClient client, final NetworkPublication publication)
    {
        this.registrationId = registrationId;
        this.client = client;

        this.publication = publication;
        publication.incRef();
    }

    @Impure
    PublicationLink(final long registrationId, final AeronClient client, final IpcPublication publication)
    {
        this.registrationId = registrationId;
        this.client = client;

        this.publication = publication;
        publication.incRef();
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void close()
    {
        if (publication instanceof NetworkPublication)
        {
            ((NetworkPublication)publication).decRef();
        }
        else
        {
            ((IpcPublication)publication).decRef();
        }
    }

    @Impure
    void revoke()
    {
        if (publication instanceof NetworkPublication)
        {
            ((NetworkPublication)publication).revoke();
        }
        else
        {
            ((IpcPublication)publication).revoke();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Impure
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (client.hasTimedOut())
        {
            reachedEndOfLife = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Pure
    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    @Pure
    long registrationId()
    {
        return registrationId;
    }
}
