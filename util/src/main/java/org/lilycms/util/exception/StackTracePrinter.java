package org.lilycms.util.exception;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * Print stack traces with knowledge about {@link RemoteThrowableInfo}.
 */
public class StackTracePrinter {
    public static void printStackTrace(Throwable throwable) {
        printStackTrace(throwable, System.err);
    }

    public static void printStackTrace(Throwable throwable, PrintStream ps) {
        ps.println(getClassName(throwable) + ": " + getMessage(throwable));
        printRemoteWarning(throwable, ps);

        List<StackTraceElement> trace = getStackTrace(throwable);

        for (StackTraceElement aTrace : trace)
            ps.println("\tat " + aTrace);

        printNestedStackTrace(throwable, ps);

    }

    private static void printNestedStackTrace(Throwable throwable, PrintStream ps) {
        Throwable cause = throwable.getCause();
        if (cause == null)
            return;

        ps.println(getClassName(cause) + ": " + getMessage(cause));
        printRemoteWarning(cause, ps);

        List<StackTraceElement> parentTrace = getStackTrace(throwable);
        List<StackTraceElement> causeTrace = getStackTrace(cause);

        int i = parentTrace.size() - 1;
        int j = causeTrace.size() - 1;
        for (; i >= 0 && j >= 0; i--, j--) {
            if (!parentTrace.get(i).equals(causeTrace.get(j))) {
                break;
            }
        }

        for (int k = 0; k < j + 1; k++) {
            ps.println("\tat " + causeTrace.get(k));
        }

        int common = parentTrace.size() - (i + 1);
        if (common > 0) {
            ps.println("\t... " + common + " more");
        }

        printNestedStackTrace(cause, ps);
    }

    private static void printRemoteWarning(Throwable throwable, PrintStream ps) {
        if (throwable instanceof RemoteThrowableInfo) {
            ps.println("\tWARNING: This is reproduced information of a remote exception.");
            ps.println("\t         This exception did not occur in this JVM!");
        }
    }

    private static List<StackTraceElement> getStackTrace(Throwable throwable) {
        if (throwable instanceof RemoteThrowableInfo) {
            return ((RemoteThrowableInfo)throwable).getOriginalStackTrace();
        } else {
            return Arrays.asList(throwable.getStackTrace());
        }
    }

    private static String getClassName(Throwable throwable) {
        if (throwable instanceof RemoteThrowableInfo) {
            return ((RemoteThrowableInfo)throwable).getOriginalClass();
        } else {
            return throwable.getClass().getName();
        }
    }

    private static String getMessage(Throwable throwable) {
        if (throwable instanceof RemoteThrowableInfo) {
            return ((RemoteThrowableInfo)throwable).getOriginalMessage();
        } else {
            return throwable.getMessage();
        }
    }
}
