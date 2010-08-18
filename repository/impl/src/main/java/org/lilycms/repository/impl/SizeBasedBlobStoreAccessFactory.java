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

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobStoreAccess;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.util.ArgumentValidator;

public class SizeBasedBlobStoreAccessFactory implements BlobStoreAccessFactory {

    private final SortedMap<Long, BlobStoreAccess> blobStoreAccesses = new TreeMap<Long, BlobStoreAccess>();
    
    public SizeBasedBlobStoreAccessFactory(BlobStoreAccess defaultBlobStoreAccess) {
        ArgumentValidator.notNull(defaultBlobStoreAccess, "defaultBlobStoreAccess");
        blobStoreAccesses.put(Long.MAX_VALUE, defaultBlobStoreAccess);
    }

    public void addBlobStoreAccess(long upperLimit, BlobStoreAccess blobStoreAccess) {
        blobStoreAccesses.put(upperLimit, blobStoreAccess);
    }
    
    public BlobStoreAccess get(Blob blob) {
        Long size = blob.getSize();
        for (Long upperLimit: blobStoreAccesses.keySet()) {
            if (size <= upperLimit) {
                 return blobStoreAccesses.get(upperLimit);
            }
        }
        return blobStoreAccesses.get(Long.MAX_VALUE);
    }
    
    public Collection<BlobStoreAccess> getAll() {
        return blobStoreAccesses.values();
    }
}
