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
package io.aeron.config;

import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.Impure;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Indicates the 'type' of the default field/value.
 */
public enum DefaultType
{
    /**
     * Type is undefined.
     */
    UNDEFINED("", "", false),
    /**
     * Boolean type.
     */
    BOOLEAN("java.lang.Boolean", "Boolean", false),
    /**
     * Int32 type.
     */
    INT("java.lang.Integer", "Integer", true),
    /**
     * Int64 type.
     */
    LONG("java.lang.Long", "Long", true),
    /**
     * Floating point type.
     */
    DOUBLE("java.lang.Double", "Double", true),
    /**
     * String type.
     */
    STRING("java.lang.String", "String", false);

    private static final Map<String, DefaultType> BY_CANONICAL_NAME = new HashMap<>();

    static
    {
        for (final DefaultType t : values())
        {
            BY_CANONICAL_NAME.put(t.canonicalName, t);
        }
    }

    /**
     * @param canonicalName the name of the java class.
     * @return the associated DefaultType.
     */
    @Pure
    public static DefaultType fromCanonicalName(final String canonicalName)
    {
        return BY_CANONICAL_NAME.getOrDefault(canonicalName, UNDEFINED);
    }

    /**
     * @param defaultType a DefaultType or null.
     * @return true if the type is null or if it's UNDEFINED, otherwise false.
     */
    @Pure
    public static boolean isUndefined(final DefaultType defaultType)
    {
        return Objects.isNull(defaultType) || UNDEFINED == defaultType;
    }

    private final String canonicalName;
    private final String simpleName;
    private final boolean numeric;

    @Impure
    DefaultType(final String canonicalName, final String simpleName, final boolean numeric)
    {
        this.canonicalName = canonicalName;
        this.simpleName = simpleName;
        this.numeric = numeric;
    }

    /**
     * @return indicates whether the value is numeric (int or long).
     */
    @Pure
    public boolean isNumeric()
    {
        return this.numeric;
    }

    /**
     * @return a simple name, for display purposes.
     */
    @Pure
    public String getSimpleName()
    {
        return this.simpleName;
    }
}
