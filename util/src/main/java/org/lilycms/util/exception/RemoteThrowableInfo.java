/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.util.exception;

import java.util.List;

/**
 * This interface provides a mechanism to restore information about exceptions
 * which happened in some remote VM and are reproduced in the local VM for
 * informational/debugging purposes.
 *
 * <p>The idea is that the remote process, when an exception occurs, streams
 * detailed information about the exception to the client, including the causes
 * and their stacktraces.
 *
 * <p>These exceptions can then be restored in the local VM to some extent.
 * Since you can't actually force the stack trace information or the exception
 * class to be the original one, this information is delivered by the methods
 * in this interface.
 *
 * <p>This information should then be used by utilities that display the
 * exception trace, for example {@link StackTracePrinter}.
 *
 * <p>For cases where the stack trace is not printed using special utilities
 * that understand this interface, the message of the restored exception
 * should contain clear information stating that it is a restored remote
 * exception.
 *
 */
public interface RemoteThrowableInfo {
    String getOriginalMessage();

    String getOriginalClass();

    List<StackTraceElement> getOriginalStackTrace();
}
