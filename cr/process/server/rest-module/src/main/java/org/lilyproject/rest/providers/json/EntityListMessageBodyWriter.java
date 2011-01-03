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
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.rest.EntityList;
import org.lilyproject.rest.RepositoryEnabled;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.tools.import_.json.EntityWriter;
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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@Provider
public class EntityListMessageBodyWriter extends RepositoryEnabled implements MessageBodyWriter<EntityList> {

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type.equals(EntityList.class) && isTypeParamSupported(genericType) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE);
    }

    public boolean isTypeParamSupported(Type genericType) {
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)genericType;
            Type[] types = pt.getActualTypeArguments();
            if (types.length == 1 && EntityRegistry.SUPPORTED_TYPES.containsKey(types[0])) {
                return true;
            }
        }

        return false;
    }

    protected EntityWriter getEntityWriter(Type genericType) {
        Class kind = (Class)((ParameterizedType)genericType).getActualTypeArguments()[0];
        return EntityRegistry.SUPPORTED_TYPES.get(kind).getWriter();
    }

    public long getSize(EntityList entityList, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(EntityList entityList, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {

        try {
            ObjectNode listNode = JsonNodeFactory.instance.objectNode();
            ArrayNode resultsNode = listNode.putArray("results");

            EntityWriter writer = getEntityWriter(genericType);
            for (Object entity : entityList.getItems()) {
                resultsNode.add(writer.toJson(entity, repository));
            }

            JsonFormat.serialize(listNode, new CloseShieldOutputStream(entityStream));
        } catch (Throwable e) {
            // We catch every throwable, since otherwise no one does it and we will not have any trace
            // of Errors that happened.
            throw new ResourceException("Error serializing entity list.", e, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
