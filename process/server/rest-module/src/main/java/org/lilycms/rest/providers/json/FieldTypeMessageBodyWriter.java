package org.lilycms.rest.providers.json;

import org.lilycms.repository.api.FieldType;
import org.lilycms.tools.import_.json.FieldTypeWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
public class FieldTypeMessageBodyWriter implements MessageBodyWriter<FieldType> {
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return FieldType.class.isAssignableFrom(type) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE);
    }

    public long getSize(FieldType fieldType, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(FieldType fieldType, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {

        JsonFormat.serialize(FieldTypeWriter.toJson(fieldType), entityStream);
    }
}
