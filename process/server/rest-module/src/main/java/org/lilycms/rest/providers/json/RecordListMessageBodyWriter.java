package org.lilycms.rest.providers.json;

import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilycms.repository.api.Record;
import org.lilycms.repository.api.Repository;
import org.lilycms.repository.api.RepositoryException;
import org.lilycms.rest.RecordList;
import org.lilycms.rest.ResourceException;
import org.lilycms.rest.json.RecordWriter;
import org.springframework.beans.factory.annotation.Autowired;

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
public class RecordListMessageBodyWriter implements MessageBodyWriter<RecordList> {
    private Repository repository;

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return type.equals(RecordList.class) && mediaType.equals(MediaType.APPLICATION_JSON_TYPE);
    }

    public long getSize(RecordList recordList, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    public void writeTo(RecordList recordList, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException {
        
        ObjectNode listNode = JsonNodeFactory.instance.objectNode();
        ArrayNode resultsNode = listNode.putArray("results");

        try {
            for (Record record : recordList.getRecords()) {
                resultsNode.add(RecordWriter.toJson(record, repository.getTypeManager()));
            }
        } catch (RepositoryException e) {
            throw new ResourceException("Error serializing record.", e, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        JsonFormat.serialize(listNode, entityStream);
    }

    @Autowired
    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
