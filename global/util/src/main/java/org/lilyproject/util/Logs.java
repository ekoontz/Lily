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
package org.lilyproject.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Logs {
    public static void logThreadJoin(Thread thread) {
        Log log = LogFactory.getLog("org.lilyproject.threads.join");
        if (!log.isInfoEnabled())
            return;

        String context = "";
        Exception e = new Exception();
        e.fillInStackTrace();
        StackTraceElement[] stackTrace = e.getStackTrace();
        if (stackTrace.length >= 2) {
            context = stackTrace[1].toString();
        }

        log.info("Waiting for thread to die: " + thread + " at " + context);
    }
}
