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
package io.aeron.config.validation;

import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Impure;
import io.aeron.config.ConfigInfo;
import io.aeron.config.DefaultType;
import io.aeron.config.ExpectedCConfig;
import io.aeron.validation.Grep;

import java.util.Collection;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Validator
{
    @Impure
    static ValidationReport validate(
        final Collection<ConfigInfo> configInfoCollection,
        final String sourceDir)
    {
        return new Validator(sourceDir).validate(configInfoCollection).report;
    }

    private final String sourceDir;
    private final ValidationReport report;

    @SideEffectFree
    @Impure
    private Validator(final String sourceDir)
    {
        this.sourceDir = sourceDir;
        this.report = new ValidationReport();
    }

    @Impure
    private Validator validate(final Collection<ConfigInfo> configInfoCollection)
    {
        configInfoCollection.forEach(this::validateCExpectations);

        // TODO look through C code in 'sourceDir' and check for anything marked '_ENV_VAR' that hasn't been processed.
        // These will be 'C only' config options that need to be accounted for...
        // ... perhaps in the Java code we can add @Config annotations to some static final fields that Java ignores.

        return this;
    }

    @Impure
    private void validateCExpectations(final ConfigInfo configInfo)
    {
        report.addEntry(configInfo, this::validateCEnvVar, this::validateCDefault);
    }

    @Impure
    private void validateCEnvVar(final Validation validation, final ExpectedCConfig c)
    {
        if (Objects.isNull(c.envVarFieldName))
        {
            return;
        }

        /* Expectations:
         * #define AERON_OPTION_ENV_VAR "AERON_OPTION"
         */
        final String pattern = "#define[ \t]+" + c.envVarFieldName + "[ \t]+\"" + c.envVar + "\"";
        final Grep grep = Grep.execute(pattern, sourceDir);
        if (grep.success())
        {
            validation.valid("Expected Env Var found in " + grep.getFilenameAndLine());
        }
        else
        {
            validation.invalid("Expected Env Var NOT found.  `grep` command:\n" + grep.getCommandString());
        }
    }

    @Impure
    private void validateCDefault(final Validation validation, final ExpectedCConfig c)
    {
        if (Objects.isNull(c.defaultFieldName))
        {
            return;
        }

        /* Expectations:
         * #define AERON_OPTION_DEFAULT ("some_string")
         * #define AERON_OPTION_DEFAULT (1234)
         * #define AERON_OPTION_DEFAULT (10 * 1024)
         * #define AERON_OPTION_DEFAULT (1024 * INT64_C(1000))
         * #define AERON_OPTION_DEFAULT false
         * #define AERON_OPTION_DEFAULT (true)
         */
        final String pattern = "#define[ \t]+" + c.defaultFieldName;

        final Grep grep = Grep.execute(pattern, sourceDir);
        if (!grep.success())
        {
            validation.invalid("Expected Default NOT found.  `grep` command:\n" + grep.getCommandString());
            return;
        }
        final String location = grep.getFilenameAndLine();

        final Matcher matcher = Pattern.compile(pattern + "(.*)$").matcher(grep.getOutput());
        if (!matcher.find())
        {
            validation.invalid("Found Default but the pattern doesn't match at " + location);
            return;
        }

        final String originalFoundDefaultString = matcher.group(1).trim();

        if (c.defaultValueType == DefaultType.STRING)
        {
            validateCDefaultString(validation, c, originalFoundDefaultString, location);
        }
        else if (c.defaultValueType == DefaultType.BOOLEAN)
        {
            validateCDefaultBoolean(validation, c, originalFoundDefaultString, location);
        }
        else if (c.defaultValueType.isNumeric())
        {
            validateCDefaultNumeric(validation, c, originalFoundDefaultString, location);
        }
        else
        {
            validation.invalid("bad default type");
        }
    }

    @Impure
    private void validateCDefaultString(
        final Validation validation,
        final ExpectedCConfig c,
        final String originalFoundDefaultString,
        final String location)
    {
        final String foundDefaultString = originalFoundDefaultString
            .replaceFirst("^\\(", "")
            .replaceFirst("\\)$", "")
            .replaceFirst("^\"", "")
            .replaceFirst("\"$", "");

        if (foundDefaultString.equals(c.defaultValue))
        {
            validation.valid("Expected Default (\"" + foundDefaultString + "\") found in " + location);
        }
        else
        {
            validation.invalid("Expected Default string doesn't match.  Expected '" + c.defaultValue +
                "' but found '" + foundDefaultString + "' in " + location);
        }
    }

    @Impure
    private void validateCDefaultBoolean(
        final Validation validation,
        final ExpectedCConfig c,
        final String originalFoundDefaultString,
        final String location)
    {
        final String foundDefaultString = originalFoundDefaultString
            .replaceFirst("^\\(", "")
            .replaceFirst("\\)$", "");

        if (foundDefaultString.equals(c.defaultValue))
        {
            validation.valid("Expected Default '" + foundDefaultString + "' found in " + location);
        }
        else
        {
            validation.invalid("boolean doesn't match: " + location);
        }
    }

    @Impure
    private void validateCDefaultNumeric(
        final Validation validation,
        final ExpectedCConfig c,
        final String originalFoundDefaultString,
        final String location)
    {
        final String foundDefaultString = originalFoundDefaultString
            .replaceAll("INT64_C", "")
            .replaceAll("UINT32_C", "")
            .replaceAll("([0-9]+)LL", "$1")
            .replaceAll("([0-9]+)L", "$1");

        if (foundDefaultString.equals(c.defaultValue))
        {
            validation.valid("Expected Default '" + foundDefaultString + "' found in " + location);
        }
        else
        {
            validation.invalid("found " + foundDefaultString + " but expected " + c.defaultValue);
        }
    }
}
