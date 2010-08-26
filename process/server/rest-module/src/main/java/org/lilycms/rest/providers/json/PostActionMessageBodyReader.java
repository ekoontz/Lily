package org.lilycms.rest.providers.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.*;
import org.lilycms.rest.PostAction;
import org.lilycms.rest.ResourceException;
import org.lilycms.rest.json.FieldTypeReader;
import org.lilycms.rest.json.JsonFormatException;
import org.lilycms.rest.json.RecordReader;
import org.lilycms.rest.json.RecordTypeReader;
import org.lilycms.util.repo.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;

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
import java.util.HashSet;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

@Provider
public class PostActionMessageBodyReader implements MessageBodyReader<PostAction> {
    private Repository repository;

    private static Set<Type> SUPPORTED_TYPES;
    static {
        SUPPORTED_TYPES = new HashSet<Type>();
        SUPPORTED_TYPES.add(RecordType.class);
        SUPPORTED_TYPES.add(FieldType.class);
        SUPPORTED_TYPES.add(Record.class);
    }

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        if (type.equals(PostAction.class) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE)) {
            if (genericType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType)genericType;
                Type[] types = pt.getActualTypeArguments();
                if (types.length == 1 && SUPPORTED_TYPES.contains(types[0])) {
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
            if (entityType.equals(RecordType.class)) {
                ObjectNode objectNode = JsonUtil.getObject(postNode, "recordType");
                entity = RecordTypeReader.fromJson(objectNode, repository.getTypeManager());
            } else if (entityType.equals(FieldType.class)) {
                ObjectNode objectNode = JsonUtil.getObject(postNode, "fieldType");
                entity = FieldTypeReader.fromJson(objectNode, repository.getTypeManager());
            } else if (entityType.equals(Record.class)) {
                ObjectNode objectNode = JsonUtil.getObject(postNode, "record");
                entity = RecordReader.fromJson(objectNode, repository);
            } else {
                throw new RuntimeException("Unexpected PostAction parameter type: " + entityType);
            }
        } catch (JsonFormatException e) {
            throw new ResourceException("Error in submitted JSON.", e, BAD_REQUEST.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error reading submitted JSON.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }

        return new PostAction(action, entity);
    }

    @Autowired
    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
