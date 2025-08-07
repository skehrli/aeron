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
package io.aeron.version;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Version information for a specific component.
 */
public interface Version
{
    /**
     * @return major version portion.
     */
    @Pure
    int majorVersion();

    /**
     * @return minor version portion.
     */
    @Pure
    int minorVersion();

    /**
     * @return patched version portion.
     */
    @Pure
    int patchVersion();

    /**
     * @return git SHA.
     */
    @Pure
    String gitSha();
}
