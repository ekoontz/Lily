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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.repository.api.*;
import org.lilyproject.rest.RepositoryEnabled;
import org.lilyproject.rest.ResourceException;
import org.lilyproject.tools.import_.json.JsonFormatException;
import org.lilyproject.util.json.JsonFormat;

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
        if (mediaType.equals(MediaType.APPLICATION_JSON_TYPE)) {
            for (Class clazz : EntityRegistry.SUPPORTED_TYPES.keySet()) {
                if (type.isAssignableFrom(clazz))
                    return true;
            }
        }
        return false;
    }

    public Object readFrom(Class<Object> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
            throws IOException, WebApplicationException {

        JsonNode node = JsonFormat.deserializeNonStd(entityStream);

        if (!(node instanceof ObjectNode)) {
            throw new ResourceException("Request body should be a JSON object.", BAD_REQUEST.getStatusCode());
        }

        ObjectNode objectNode = (ObjectNode)node;

        try {
            return EntityRegistry.findReader(type).fromJson(objectNode, repository);
        } catch (JsonFormatException e) {
            throw new ResourceException("Error in submitted JSON.", e, BAD_REQUEST.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error reading submitted JSON.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
