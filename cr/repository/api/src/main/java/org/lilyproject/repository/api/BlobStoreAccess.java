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
package org.lilyproject.repository.api;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * The BlobStoreAccess provides access to a specific underlying blob store. This blob store must be able to store
 * a stream of bytes and be able to read or delete them again based on a key provided by the blob store itself.
 * 
 * <p>For each blob store (e.g. based on HDFS, HBase, ...) an implementation of this interface needs to be
 * registered on the repository with {@link Repository#registerBlobStoreAccess(BlobStoreAccess)}.
 */
public interface BlobStoreAccess {

    /**
     * The id is used within the repository to uniquely identify the blobstore.
     * Two blobstores should thus not use the same id.
     */
    String getId();

    /**
     * Get an {@link OutputStream} to write bytes to the blobstore.
     *
     * <p>The blobstore should generate a unique key to enable it to retrieve the written bytes again with
     * {@link #getInputStream(byte[])}. When {@link OutputStream#close()} is called the blobstore should write
     * the generated key in the value of the given Blob with {@link Blob#setValue(byte[])}.
     *
     * @param blob the blob for which an {@link OutputStream} should be provided and in which the generated key
     *             should be written.
     * @return an OutputStream to which a stream of bytes can be written
     *
     * @throws BlobException when an unexpected exception occurred (e.g. an IOException of the underlying blobstore)
     */
    OutputStream getOutputStream(Blob blob) throws BlobException;

    /**
     * Get an {@link InputStream} based to read a stream of bytes from the blobstore for the given key.
     *
     * @param key a unique key identifying the written bytes on the blobstore, see {@link #getOutputStream(Blob)}
     * 
     * @return an InputStream from whih a stream of bytes can be read
     * @throws BlobException when an unexpected exception occurred (e.g. an IOException of the underlying blobstore)
     */
    InputStream getInputStream(byte[] key) throws BlobException;

    /**
     * Delete the bytes identified by the key from the blobstore
     *
     * @param key a unique key identifying the written bytes on the blobstore, see {@link #getOutputStream(Blob)}
     *
     * @throws BlobException when an unexpected exception occurred (e.g. an IOException of the underlying blobstore)
     */
    void delete(byte[] key) throws BlobException;
}
