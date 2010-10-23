package org.lilyproject.rest;

import org.apache.commons.io.IOUtils;
import org.lilyproject.repository.api.*;
import org.lilyproject.util.io.Closer;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("record/{id}/version/{version:\\d+}/field/{fieldName}/data")
public class BlobByVersionAndFieldResource extends RepositoryEnabled {

    @GET
    @Produces("*/*")
    public Response get(@PathParam("id") String id, @PathParam("version") String version,
            @PathParam("fieldName") String fieldName, @Context UriInfo uriInfo) {
        return getBlob(id, version, fieldName, uriInfo, repository);
    }


    protected static Response getBlob(String id, String version, String fieldName, UriInfo uriInfo,
            final Repository repository) {
        RecordId recordId = repository.getIdGenerator().fromString(id);

        QName fieldQName = ResourceClassUtil.parseQName(fieldName, uriInfo.getQueryParameters());

        Long versionNr = null;
        if (version != null) {
            versionNr = Long.parseLong(version);
        }

        Record record;
        try {
            record = repository.read(recordId, versionNr, Collections.singletonList(fieldQName));

            if (!record.hasField(fieldQName)) {
                throw new ResourceException("Record " + id + " has no field " + fieldQName, NOT_FOUND.getStatusCode());
            }

            Object value = record.getField(fieldQName);
            if (!(value instanceof Blob)) {
                throw new ResourceException("Specified field is not a blob field. Record " + id + ", field " +
                        fieldQName, BAD_REQUEST.getStatusCode());
            }

            final Blob blob = (Blob)value;

            StreamingOutput output = new StreamingOutput() {
                public void write(OutputStream output) throws IOException, WebApplicationException {
                    InputStream is = null;
                    try {
                        is = repository.getInputStream(blob);
                        IOUtils.copyLarge(is, output);
                    } catch (BlobException e) {
                        throw new ResourceException(e, INTERNAL_SERVER_ERROR.getStatusCode());
                    } catch (BlobNotFoundException e) {
                        throw new ResourceException(e, NOT_FOUND.getStatusCode());
                    } finally {
                        Closer.close(is);
                    }
                }
            };

            return Response.ok(output, MediaType.valueOf(blob.getMimetype())).build();

        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
