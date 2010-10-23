package org.lilyproject.rest.providers.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.util.json.JsonFormat;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Throwable> {

    public Response toResponse(Throwable throwable) {
        int status = 500;
        Throwable mainThrowable = throwable;

        if (throwable instanceof ResourceException) {
            ResourceException re = (ResourceException)throwable;

            status = re.getStatus();

            // If the exception only serves to annotate another exception with a status code...
            if (re.getSpecificMessage() == null && re.getCause() != null) {
                if (re.getCause() != null) {
                    mainThrowable = re.getCause();
                }
            } else {
                mainThrowable = throwable;
            }

        }

        final ObjectNode msgNode = JsonNodeFactory.instance.objectNode();
        msgNode.put("status", status);

        String description = mainThrowable.getMessage();
        if (description == null) {
            Response.Status statusObject = Response.Status.fromStatusCode(status);
            description =  statusObject != null ? statusObject.toString() : "No error description, unknown status code: " + status;
        }

        msgNode.put("description", description);

        msgNode.put("causes", causesToJson(mainThrowable));

        StreamingOutput entity = new StreamingOutput() {
            public void write(OutputStream output) throws IOException, WebApplicationException {
                JsonFormat.serialize(msgNode, output);
            }
        };

        return Response.status(status).type(MediaType.APPLICATION_JSON_TYPE).entity(entity).build();
    }

    private ArrayNode causesToJson(Throwable throwable) {
        ArrayNode causesNode = JsonNodeFactory.instance.arrayNode();

        Throwable parentThrowable = null;

        while (throwable != null) {
            ObjectNode causeNode = causesNode.addObject();
            causeNode.put("message", throwable.getMessage());
            causeNode.put("type", throwable.getClass().getName());

            if (parentThrowable == null) {
                addStackTraceToJson(throwable.getStackTrace(), causeNode);
            } else {
                addStackTraceToJson(parentThrowable.getStackTrace(), throwable.getStackTrace(), causeNode);
            }

            parentThrowable = throwable;        
            throwable = throwable.getCause();
        }

        return causesNode;
    }

    private void addStackTraceToJson(StackTraceElement[] stackTrace, ObjectNode parentNode) {
        ArrayNode stackTraceNode = JsonNodeFactory.instance.arrayNode();
        for (StackTraceElement ste : stackTrace) {
            stackTraceNode.add(stackTraceElementToJson(ste));
        }
        parentNode.put("stackTrace", stackTraceNode);
    }

    private void addStackTraceToJson(StackTraceElement[] parentTrace, StackTraceElement[] stackTrace, ObjectNode parentNode) {

        int i = parentTrace.length - 1;
        int j = stackTrace.length - 1;
        for (; i >= 0 && j >= 0; i--, j--) {
            if (!parentTrace[i].equals(stackTrace[j])) {
                break;
            }
        }

        ArrayNode stackTraceNode = parentNode.putArray("stackTrace");
        for (int k = 0; k < j + 1; k++) {
            stackTraceNode.add(stackTraceElementToJson(stackTrace[k]));
        }

        int common = parentTrace.length - (i + 1);
        if (common > 0) {
            parentNode.put("stackTraceCommon", common);
        }
    }

    private ObjectNode stackTraceElementToJson(StackTraceElement ste) {
        ObjectNode steNode = JsonNodeFactory.instance.objectNode();
        steNode.put("class", ste.getClassName());
        steNode.put("method", ste.getMethodName());
        if (ste.getFileName() != null)
            steNode.put("file", ste.getFileName());
        steNode.put("line", ste.getLineNumber());
        if (ste.isNativeMethod())
            steNode.put("native", ste.isNativeMethod());
        return steNode;
    }
}
