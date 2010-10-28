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

            return Response.ok(output, MediaType.valueOf(blob.getMediaType())).build();

        } catch (RecordNotFoundException e) {
            throw new ResourceException(e, NOT_FOUND.getStatusCode());
        } catch (RepositoryException e) {
            throw new ResourceException("Error loading record.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

}
