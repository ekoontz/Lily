package org.lilycms.rest.providers.json;

import org.lilycms.repository.api.Blob;
import org.lilycms.tools.import_.json.BlobConverter;

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
public class BlobMessageBodyWriter implements MessageBodyWriter<Blob> {
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type.equals(Blob.class) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE);
    }

    public long getSize(Blob blob, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(Blob blob, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {

        JsonFormat.serialize(BlobConverter.toJson(blob), entityStream);
    }
}
