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
