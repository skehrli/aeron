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
package io.aeron.validation;

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.dataflow.qual.SideEffectFree;
import org.checkerframework.dataflow.qual.Pure;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A utility class for running 'grep'.
 */
public final class Grep
{
    /**
     * @param pattern the regex pattern passed to grep.
     * @param sourceDir the base directory where the search should begin.
     * @return a Grep object with the results of the action.
     */
    @Impure
    public static Grep execute(final String pattern, final String sourceDir)
    {
        final String commandString = "grep -r -n -E '^" + pattern + "' " + sourceDir;

        try
        {
            // TODO make grep location configurable??
            final Process process = new ProcessBuilder()
                .redirectErrorStream(true)
                .command(new String[] {"/usr/bin/grep", "-r", "-n", "-E", "^" + pattern, sourceDir})
                .start();

            final int exitCode = process.waitFor();

            try (
                InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream());
                BufferedReader reader = new BufferedReader(inputStreamReader))
            {
                return new Grep(commandString, exitCode, reader.lines().collect(Collectors.toList()));
            }
        }
        catch (final Exception e)
        {
            return new Grep(commandString, e);
        }
    }

    private final String commandString;

    private final int exitCode;

    private final List<String> lines;

    private final Exception e;

    @SideEffectFree
    private Grep(final String commandString, final Exception e)
    {
        this.commandString = commandString;
        this.exitCode = -1;
        this.lines = Collections.emptyList();
        this.e = e;
    }

    @SideEffectFree
    private Grep(final String commandString, final int exitCode, final List<String> lines)
    {
        this.commandString = commandString;
        this.exitCode = exitCode;
        this.lines = lines;
        this.e = null;
    }

    /**
     * @return whether grep succeeded.
     */
    @Pure
    @Impure
    public boolean success()
    {
        return success(true);
    }

    /**
     * @param expectOneLine many of the usages expect only a single line to be found.
     *                      if more than one are found, that counts as a failure
     * @return whether grep succeeded.
     */
    @Pure
    public boolean success(final boolean expectOneLine)
    {
        if (this.e != null)
        {
            return false;
        }

        if (this.exitCode != 0)
        {
            return false;
        }

        return !expectOneLine || this.lines.size() == 1;
    }

    /**
     * @return the command string that was executed
     */
    @Pure
    public String getCommandString()
    {
        return this.commandString;
    }

    /**
     * @return the filename and line number of the first line of output
     */
    @SideEffectFree
    @Impure
    public String getFilenameAndLine()
    {
        return getFilenameAndLine(0);
    }

    /**
     * @param lineNumber specify the line of output
     * @return the filename and line number of the specified line of output
     */
    @SideEffectFree
    public String getFilenameAndLine(final int lineNumber)
    {
        final String[] pieces = this.lines.get(lineNumber).split(":");
        return pieces[0] + ":" + pieces[1];
    }

    /**
     * @return the first line of output (minus the filename and line number)
     */
    @SideEffectFree
    @Impure
    public String getOutput()
    {
        return getOutput(0);
    }

    /**
     * @param lineNumber specify the line of output
     * @return the output of the specified line number (minus the filename and the line number)
     */
    @SideEffectFree
    public String getOutput(final int lineNumber)
    {
        return this.lines.get(lineNumber).split(":")[2];
    }

    /**
     * @param action a BiConsumer that consumes the filename/line number and output for each line of output
     */
    @Impure
    public void forEach(final BiConsumer<String, String> action)
    {
        for (int i = 0; i < lines.size(); i++)
        {
            action.accept(getFilenameAndLine(i), getOutput(i));
        }
    }
}
