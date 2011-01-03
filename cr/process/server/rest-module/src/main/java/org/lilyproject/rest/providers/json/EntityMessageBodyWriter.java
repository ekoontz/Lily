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
package org.lilyproject.rest.providers.json;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.lilyproject.rest.RepositoryEnabled;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.util.json.JsonFormat;

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
public class EntityMessageBodyWriter extends RepositoryEnabled implements MessageBodyWriter<Object> {
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (mediaType.equals(MediaType.APPLICATION_JSON_TYPE)) {
            for (Class clazz : EntityRegistry.SUPPORTED_TYPES.keySet()) {
                if (clazz.isAssignableFrom(type))
                    return true;
            }
        }
        return false;
    }

    public long getSize(Object o, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(Object object, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        try {
            JsonFormat.serialize(EntityRegistry.findWriter(object.getClass()).toJson(object, repository),
                    new CloseShieldOutputStream(entityStream));
        } catch (Throwable e) {
            // We catch every throwable, since otherwise no one does it and we will not have any trace
            // of Errors that happened.
            throw new ResourceException("Error serializing entity.", e, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
