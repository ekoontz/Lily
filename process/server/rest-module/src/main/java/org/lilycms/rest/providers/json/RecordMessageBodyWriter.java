package org.lilycms.rest.providers.json;

import org.lilycms.repository.api.*;
import org.lilycms.rest.RepositoryEnabled;
import org.lilycms.rest.ResourceException;
import org.lilycms.rest.json.RecordWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
public class RecordMessageBodyWriter extends RepositoryEnabled implements MessageBodyWriter<Record> {

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Record.class.isAssignableFrom(type) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE);
    }

    public long getSize(Record record, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(Record record, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {

        try {
            JsonFormat.serialize(RecordWriter.toJson(record, repository.getTypeManager()), entityStream);
        } catch (RepositoryException e) {
            throw new ResourceException("Error serializing record.", e, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}

