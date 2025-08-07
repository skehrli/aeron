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
import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * Annotation to indicate this is a config option.
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface Config
{
    /**
     * Type is used to indicate whether the annotation is marking a property name or a default value.
     */
    enum Type
    {
        /**
         * Undefined.
         */
        UNDEFINED,
        /**
         * Property.
         */
        PROPERTY_NAME,
        /**
         * Default value.
         */
        DEFAULT
    }

    /**
     * @return what type of field is being annotated.
     */
    @Pure
    Type configType() default Type.UNDEFINED;

    /**
     * @return the unique id that ties together all the usages of the annotation across fields/methods.
     */
    @Pure
    String id() default "";

    /**
     * @return the uri parameter (if any) associated with this option.
     */
    @Pure
    String uriParam() default "";

    /**
     * @return whether this config option exists in the C code.
     */
    @Pure
    boolean existsInC() default true;

    /**
     * @return the expected C #define name that will be set with the env variable name for this option.
     */
    @Pure
    String expectedCEnvVarFieldName() default "";

    /**
     * @return the expected C env variable name for this option.
     */
    @Pure
    String expectedCEnvVar() default "";

    /**
     * @return the expected C #define name that will be set with the default value for this option.
     */
    @Pure
    String expectedCDefaultFieldName() default "";

    /**
     * @return the expected C default value for this option.
     */
    @Pure
    String expectedCDefault() default "";

    /**
     * @return whether to skip validation of the default in C.
     */
    @Pure
    boolean skipCDefaultValidation() default false;

    /**
     * @return what's the type of default (string, int, etc...).
     */
    @Pure
    DefaultType defaultType() default DefaultType.UNDEFINED;

    /**
     * @return specify the default boolean, if defaultType is BOOLEAN.
     */
    @Pure
    boolean defaultBoolean() default false;

    /**
     * @return specify the default int, if defaultType is INT.
     */
    @Pure
    int defaultInt() default 0;

    /**
     * @return specify the default long, if defaultType is LONG.
     */
    @Pure
    long defaultLong() default 0;

    /**
     * @return specify the default double, if defaultType is DOUBLE.
     */
    @Pure
    double defaultDouble() default 0.0;

    /**
     * @return specify the default string, if defaultType is STRING.
     */
    @Pure
    String defaultString() default "";

    /**
     * @return specify a string that acts as a stand-in for the default value when generating documentation.
     */
    @Pure
    String defaultValueString() default "";

    /**
     * Used to indicate whether the default value is a time value.
     */
    enum IsTimeValue
    {
        /**
         * Undefined.
         */
        UNDEFINED,
        /**
         * True.
         */
        TRUE,
        /**
         * False.
         */
        FALSE
    }

    /**
     * @return whether the default value is a time value.
     */
    @Pure
    IsTimeValue isTimeValue() default IsTimeValue.UNDEFINED;

    /**
     * @return the time unit if the default value is a time value of some sort.
     */
    @Pure
    TimeUnit timeUnit() default TimeUnit.NANOSECONDS;

    /**
     * @return whether this config option has a 'context'.
     */
    @Pure
    boolean hasContext() default true;
}
