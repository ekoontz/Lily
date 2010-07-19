package org.lilycms.repository.avro;

import org.lilycms.util.exception.RemoteThrowableInfo;

import java.util.List;

public class RestoredException extends Exception implements RemoteThrowableInfo {
    private String originalClass;
    private String originalMessage;
    private List<StackTraceElement> stackTrace;

    public RestoredException(String message, String originalClass, List<StackTraceElement> stackTrace) {
        super("[remote exception of type " + originalClass + "]" + message);
        this.originalMessage = message;
        this.originalClass = originalClass;
        this.stackTrace = stackTrace;
    }

    public String getOriginalClass() {
        return originalClass;
    }

    public List<StackTraceElement> getOriginalStackTrace() {
        return stackTrace;
    }

    public String getOriginalMessage() {
        return originalMessage;
    }
}
