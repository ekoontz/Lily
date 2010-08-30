package org.lilycms.rest.providers.json;

import org.lilycms.repository.api.RepositoryException;
import org.lilycms.rest.ResourceException;
import org.lilycms.tools.import_.json.EntityWriter;

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
import java.util.Map;

@Provider
public class EntityMessageBodyWriter extends BaseEntityWriter implements MessageBodyWriter<Object> {
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        for (Class clazz : SUPPORTED_TYPES.keySet()) {
            if (clazz.isAssignableFrom(type))
                return true;
        }
        return false;
    }

    public long getSize(Object o, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(Object object, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        try {
            for (Map.Entry<Class, EntityWriter> entry : SUPPORTED_TYPES.entrySet()) {
                if (entry.getKey().isAssignableFrom(object.getClass())) {
                    JsonFormat.serialize(entry.getValue().toJson(object, repository), entityStream);                                    
                }
            }
        } catch (RepositoryException e) {
            throw new ResourceException("Error serializing entity.", e, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
