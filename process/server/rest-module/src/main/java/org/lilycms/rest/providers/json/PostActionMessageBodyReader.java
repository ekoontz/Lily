package org.lilycms.rest.providers.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;
import org.lilycms.rest.PostAction;
import org.lilycms.rest.RepositoryEnabled;
import org.lilycms.rest.ResourceException;
import org.lilycms.tools.import_.json.JsonFormatException;
import org.lilycms.util.repo.JsonUtil;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Provider
public class PostActionMessageBodyReader extends RepositoryEnabled implements MessageBodyReader<PostAction> {

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (type.equals(PostAction.class) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType)genericType;
                Type[] types = pt.getActualTypeArguments();
                if (types.length == 1 && EntityRegistry.SUPPORTED_TYPES.containsKey(types[0])) {
                    return true;
                }
            }
        }

        return false;
    }

    public PostAction readFrom(Class<PostAction> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
            throws IOException, WebApplicationException {

        Type entityType = ((ParameterizedType)genericType).getActualTypeArguments()[0];

        JsonNode node = JsonFormat.deserialize(entityStream);

        if (!(node instanceof ObjectNode)) {
            throw new ResourceException("Request body should be a JSON object.", BAD_REQUEST.getStatusCode());
        }

        ObjectNode postNode = (ObjectNode)node;
        String action = JsonUtil.getString(postNode, "action");

        Object entity;
        try {
            EntityRegistry.RegistryEntry registryEntry = EntityRegistry.findReaderRegistryEntry((Class)entityType);
            ObjectNode objectNode = JsonUtil.getObject(postNode, registryEntry.getPropertyName());
            entity = EntityRegistry.findReader((Class)entityType).fromJson(objectNode, repository);
        } catch (JsonFormatException e) {
            throw new ResourceException("Error in submitted JSON.", e, BAD_REQUEST.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error reading submitted JSON.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        return new PostAction(action, entity);
    }

}
