package org.lilycms.rest;

import org.apache.commons.io.IOUtils;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobException;
import org.lilycms.repository.api.Repository;
import org.lilycms.util.io.Closer;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import static javax.ws.rs.core.Response.Status.*;

@Path("/blob")
public class BlobCollectionResource {
    private Repository repository;

    @POST
    @Consumes("*/*")
    @Produces("application/json")
    public Response post(@Context HttpHeaders headers, InputStream is) {
        String lengthHeader = headers.getRequestHeaders().getFirst(HttpHeaders.CONTENT_LENGTH);
        if (lengthHeader == null) {
            throw new ResourceException("Content-Length header is required for uploading blobs.", BAD_REQUEST.getStatusCode());
        }

        // TODO do we want the mimetype to include the parameters?
        String mimeType = headers.getMediaType().getType() + "/" + headers.getMediaType().getSubtype();

        long length = Long.parseLong(lengthHeader);
        Blob blob = new Blob(mimeType, length, null);

        OutputStream os = null;
        try {
            os = repository.getOutputStream(blob);
            IOUtils.copyLarge(is, os);
        } catch (BlobException e) {
            throw new ResourceException("Error writing blob.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        } catch (IOException e) {
            throw new ResourceException("Error writing blob.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        } finally {
            Closer.close(os);
        }


        // TODO what URI to return?
        URI uri = UriBuilder.fromUri("/foobar").build();
        return Response.created(uri).entity(blob).build();
    }

    @Autowired
    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
