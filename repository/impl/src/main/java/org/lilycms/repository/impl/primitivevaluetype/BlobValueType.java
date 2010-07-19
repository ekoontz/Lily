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
package org.lilycms.repository.impl.primitivevaluetype;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.PrimitiveValueType;

public class BlobValueType implements PrimitiveValueType {
    private final String NAME = "BLOB";

    public String getName() {
        return NAME;
    }

    public Object fromBytes(byte[] bytes) {
        int offset = 0;
        int keyLength = Bytes.toInt(bytes);
        offset = offset + Bytes.SIZEOF_INT;
        byte[] key = null;
        if (keyLength > 0) {
            key = Arrays.copyOfRange(bytes, offset, offset + keyLength);
            offset = offset + keyLength;
        }
        int mimetypeLength = Bytes.toInt(bytes, offset);
        offset = offset + Bytes.SIZEOF_INT;
        String mimetype = Bytes.toString(bytes, offset, mimetypeLength);
        offset = offset + mimetypeLength;
        Long size = Bytes.toLong(bytes, offset);
        offset = offset + Bytes.SIZEOF_LONG;
        if (size == -1) {
            size = null;
        }
        int filenameLength = Bytes.toInt(bytes, offset);
        offset = offset + Bytes.SIZEOF_INT;
        String filename = null;
        if (filenameLength > 0) {
            filename = Bytes.toString(bytes, offset, filenameLength);
        }
        return new Blob(key, mimetype, size, filename);
    }

    public byte[] toBytes(Object value) {
        byte[] bytes = new byte[0];
        Blob blob = (Blob)value;
        byte[] key = blob.getValue();
        if (key == null) {
            bytes = Bytes.add(bytes, Bytes.toBytes((int)0));
        } else {
            bytes = Bytes.add(bytes, Bytes.toBytes(key.length));
            bytes = Bytes.add(bytes, key);
        }
        bytes = Bytes.add(bytes, Bytes.toBytes(blob.getMimetype().length()));
        bytes = Bytes.add(bytes, Bytes.toBytes(blob.getMimetype()));
        Long size = blob.getSize();
        if (size == null) {
            size = Long.valueOf(-1);
        }
        bytes = Bytes.add(bytes, Bytes.toBytes(size));
        String filename = blob.getName();
        if (filename == null) {
            bytes = Bytes.add(bytes, Bytes.toBytes(Integer.valueOf(0)));
        } else {
            bytes = Bytes.add(bytes, Bytes.toBytes(filename.length()));
            bytes = Bytes.add(bytes, Bytes.toBytes(blob.getName()));
        }
        return bytes;
    }

    public Class getType() {
        return Blob.class;
    }

}
