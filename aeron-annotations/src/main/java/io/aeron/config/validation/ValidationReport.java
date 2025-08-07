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

import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import io.aeron.config.ConfigInfo;
import io.aeron.config.ExpectedCConfig;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

final class ValidationReport
{
    private final List<Entry> entries;

    @SideEffectFree
    ValidationReport()
    {
        entries = new ArrayList<>();
    }

    @Impure
    void addEntry(
        final ConfigInfo configInfo,
        final BiConsumer<Validation, ExpectedCConfig> validateCEnvVar,
        final BiConsumer<Validation, ExpectedCConfig> validateCDefault)
    {
        final Entry entry = new Entry(configInfo);
        final ExpectedCConfig c = configInfo.expectations.c;
        if (c.exists)
        {
            validate(validateCEnvVar, entry.envVarValidation, c);

            if (c.skipDefaultValidation)
            {
                entry.defaultValidation.valid("skipped");
            }
            else
            {
                validate(validateCDefault, entry.defaultValidation, c);
            }
        }
        entries.add(entry);
    }

    @Impure
    private void validate(
        final BiConsumer<Validation, ExpectedCConfig> func,
        final @Owning Validation validation,
        final ExpectedCConfig c)
    {
        try
        {
            func.accept(validation, c);
        }
        catch (final Exception e)
        {
            validation.invalid(e.getMessage());
            e.printStackTrace(validation.out());
        }
        finally
        {
            validation.close();
        }
    }

    @Impure
    void printOn(final PrintStream out)
    {
        entries.forEach(entry -> entry.printOn(out));
    }

    @Impure
    void printFailuresOn(final PrintStream out)
    {
        entries.forEach(entry -> entry.printFailuresOn(out));
    }
}
