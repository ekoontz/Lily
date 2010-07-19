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
package org.lilycms.repository.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobException;
import org.lilycms.repository.api.BlobStoreAccess;

public class InlineBlobStoreAccess implements BlobStoreAccess {

    private static final String ID = "INLINE";
    

    public InlineBlobStoreAccess() throws IOException {
    }
    
    public String getId() {
        return ID;
    }
        
    public OutputStream getOutputStream(Blob blob) throws BlobException {
        return new InlineBlobOutputStream(blob);
    }

    public InputStream getInputStream(byte[] blobKey) throws BlobException {
        return new ByteArrayInputStream(blobKey);
    }
    
    public void delete(byte[] blobKey) {
        // no-op
    }

    private class InlineBlobOutputStream extends ByteArrayOutputStream {
        
        private final Blob blob;
        public InlineBlobOutputStream(Blob blob) {
            super();
            this.blob = blob;
        }
        @Override
        public void close() throws IOException {
            super.close();
            blob.setValue(toByteArray());
        }
    }
}
