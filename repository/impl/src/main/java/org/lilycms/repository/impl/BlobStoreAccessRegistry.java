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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.*;
import org.lilycms.repository.api.BlobNotFoundException;
import org.lilycms.util.Pair;

public class BlobStoreAccessRegistry {

    Map<String, BlobStoreAccess> registry = new HashMap<String, BlobStoreAccess>();
    private BlobStoreAccessFactory blobStoreAccessFactory;
    
    public BlobStoreAccessRegistry() {
    }
    
    public void register(BlobStoreAccess blobStoreAccess) {
        registry.put(blobStoreAccess.getId(), blobStoreAccess);
    }
    
    public void setBlobStoreAccessFactory(BlobStoreAccessFactory blobStoreAccessFactory) {
        this.blobStoreAccessFactory = blobStoreAccessFactory;
    }

    public OutputStream getOutputStream(Blob blob) throws BlobException {
        BlobStoreAccess blobStoreAccess = blobStoreAccessFactory.getBlobStoreAccess(blob);
        return new BlobOutputStream(blobStoreAccess.getOutputStream(blob), blobStoreAccess.getId(), blob);
    }

    public InputStream getInputStream(Blob blob) throws BlobNotFoundException, BlobException {
        Pair<String, byte[]> decodedKey = decodeKey(blob);
        BlobStoreAccess blobStoreAccess = registry.get(decodedKey.getV1());
        return blobStoreAccess.getInputStream(decodedKey.getV2());
    }

    private Pair<String, byte[]> decodeKey(Blob blob) throws BlobNotFoundException, BlobException {
        if (blob.getValue() == null) {
            throw new BlobNotFoundException(blob);
        }
        Pair<String, byte[]> decodedKey;
        try {
            decodedKey = decode(blob.getValue());
        } catch (Exception e) {
            throw new BlobException("Failed to decode the blobkey of the blob <" + blob + ">", e);
        }
        return decodedKey;
    }

    
    public void delete(Blob blob) throws BlobNotFoundException, BlobException {
        Pair<String, byte[]> decodedKey = decodeKey(blob);
        BlobStoreAccess blobStoreAccess = registry.get(decodedKey.getV1());
        blobStoreAccess.delete(decodedKey.getV2());
    }
    
    static private byte[] encode(String id, byte[] blobKey) {
        byte[] bytes = new byte[0];
        bytes = Bytes.add(bytes, Bytes.toBytes(id.length()));
        bytes = Bytes.add(bytes, Bytes.toBytes(id));
        bytes = Bytes.add(bytes, blobKey);
        return bytes;
    }
    
    static private Pair<String, byte[]>  decode(byte[] key) {
        int idLength = Bytes.toInt(key);
        String id = Bytes.toString(key, Bytes.SIZEOF_INT, idLength);
        byte[] blobKey = Bytes.tail(key, key.length - Bytes.SIZEOF_INT - idLength);
        return new Pair<String, byte[]>(id, blobKey);
    }
    
    private class BlobOutputStream extends FilterOutputStream {

        private final Blob blob;
        private final String blobStoreAccessId;

        public BlobOutputStream(OutputStream outputStream, String blobStoreAccessId, Blob blob) {
            super(outputStream);
            this.blobStoreAccessId = blobStoreAccessId;
            this.blob = blob;
        }

        @Override
        public void close() throws IOException {
            super.close();
            blob.setValue(encode(blobStoreAccessId, blob.getValue()));
        }
    }


}
