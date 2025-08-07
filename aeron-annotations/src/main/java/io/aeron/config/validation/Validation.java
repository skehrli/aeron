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

import org.checkerframework.dataflow.qual.Impure;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.checker.mustcall.qual.InheritableMustCall;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

@InheritableMustCall("close")
class Validation
{
    private boolean valid = false;

    private String message;

    private ByteArrayOutputStream baOut;

    @Owning
    private PrintStream psOut;

    @Pure
    boolean isValid()
    {
        return valid;
    }

    @EnsuresCalledMethods(value="this.psOut", methods="close")
    @Impure
    void close()
    {
        if (this.psOut != null)
        {
            this.psOut.close();
        }
    }

    @Impure
    void valid(final String message)
    {
        this.valid = true;
        this.message = message;
    }

    @Impure
    void invalid(final String message)
    {
        this.valid = false;
        this.message = message;
    }

    @NotOwning
    @Impure
    PrintStream out()
    {
        if (this.psOut == null)
        {
            this.baOut = new ByteArrayOutputStream();
            this.psOut = new PrintStream(baOut);
        }

        return psOut;
    }

    @Impure
    void printOn(final PrintStream out)
    {
        out.println(" " + (this.valid ? "+" : "-") + " " + this.message);
        if (this.psOut != null)
        {
            out.println(this.baOut);
        }
    }
}
