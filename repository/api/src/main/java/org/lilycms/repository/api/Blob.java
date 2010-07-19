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
package org.lilycms.repository.api;

import java.util.Arrays;

import org.lilycms.util.ArgumentValidator;

/**
 * A Blob is a {@link PrimitiveValueType primitive type} that can be used to store arbitrarily large binary data.
 * Besides the binary data itself, a blob holds some metadata such as the mime-type and size.
 *
 * <p>Storing and retrieving data to and from the blob happens through {@link java.io.OutputStream} and
 * {@link java.io.InputStream}.
 *
 * <p>These streams are available via the Repository through {@link Repository#getOutputStream(Blob)} and
 * {@link Repository#getInputStream(Blob)}.
 *
 * <p><b>Important:</b> to add a blob to a record, you first need to upload the data via the OutputStream. Once
 * finished, close the OutputStream using {@link java.io.OutputStream#close()}. This will trigger the update of
 * the {@link #setValue value} of this blob object (which will usually be a pointer to the storage location
 * of the blob, though for tiny blobs -- sorry for the oxymoron -- this might be the binary data itself).
 * Once that's done, you can store the {@link Record} itself. Thus: first upload all blobs and close their
 * OutputStreams, then save the record. It does not matter when the blob value is set on the record object itself.
 */
public class Blob {

    private final String mimetype;
    private final Long size;
    private final String name;
    private byte[] value;

    /**
     * This is the default constructor to create a Blob.
     *
     * @param mimetype the media type of the data represented by the blob.
     * @param size the size in number of bytes of the data that will be written. The size is a mandatory parameter.
     * @param name a name with no extra semantic meaning. Typically used to store a filename, e.g. for when a user
     *             downloads this blob to her desktop. Optional, can be null.
     */
    public Blob(String mimetype, Long size, String name) {
        this(null, mimetype, size, name);
    }

    /**
     * This constructor should only be used internally.
     *
     * @param value the value will be generated after data has been written to the OutputStream and the stream
     *              has been closed.
     */
    public Blob(byte[] value, String mimetype, Long size, String name) {
        ArgumentValidator.notNull(size, "size");
        this.value = value;
        this.mimetype = mimetype;
        this.size = size;
        this.name = name;
    }
    
    
    public byte[] getValue() {
        return value;
    }
    
    public void setValue(byte[] value) {
        this.value = value;
    }
    
    public String getMimetype() {
        return mimetype;
    }
    
    public Long getSize() {
        return size;
    }
    
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + Arrays.hashCode(value);
        result = prime * result + ((mimetype == null) ? 0 : mimetype.hashCode());
        result = prime * result + ((size == null) ? 0 : size.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Blob other = (Blob) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        if (mimetype == null) {
            if (other.mimetype != null)
                return false;
        } else if (!mimetype.equals(other.mimetype))
            return false;
        if (size == null) {
            if (other.size != null)
                return false;
        } else if (!size.equals(other.size))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Blob [name=" + name + ", mimetype=" + mimetype + ", size=" + size + ", value="
                        + Arrays.toString(value) + "]";
    }
}
