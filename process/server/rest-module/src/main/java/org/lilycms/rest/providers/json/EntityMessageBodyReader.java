package org.lilycms.rest.providers.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;
import org.lilycms.rest.RepositoryEnabled;
import org.lilycms.rest.ResourceException;
import org.lilycms.rest.json.FieldTypeReader;
import org.lilycms.rest.json.JsonFormatException;
import org.lilycms.rest.json.RecordReader;
import org.lilycms.rest.json.RecordTypeReader;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import static javax.ws.rs.core.Response.Status.*;

@Provider
public class EntityMessageBodyReader extends RepositoryEnabled implements MessageBodyReader<Object> {

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return mediaType.equals(MediaType.APPLICATION_JSON_TYPE) &&
                (type.equals(FieldType.class) || type.equals(RecordType.class) || type.equals(Record.class));
    }

    public Object readFrom(Class<Object> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
            throws IOException, WebApplicationException {

        JsonNode node = JsonFormat.deserialize(entityStream);

        if (!(node instanceof ObjectNode)) {
            throw new ResourceException("Request body should be a JSON object.", BAD_REQUEST.getStatusCode());
        }

        ObjectNode objectNode = (ObjectNode)node;

        try {
            if (type.equals(FieldType.class)) {                
                return FieldTypeReader.fromJson(objectNode, repository.getTypeManager());
            } else if (type.equals(RecordType.class)) {
                return RecordTypeReader.fromJson(objectNode, repository.getTypeManager());
            } else if (type.equals(Record.class)) {
                return RecordReader.fromJson(objectNode, repository);
            } else {
                throw new RuntimeException("Unexpected type: " + type);
            }
        } catch (JsonFormatException e) {
            throw new ResourceException("Error in submitted JSON.", e, BAD_REQUEST.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error reading submitted JSON.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
