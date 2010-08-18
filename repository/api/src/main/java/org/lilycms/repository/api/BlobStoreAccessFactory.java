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

import java.util.Collection;

/**
 * A BlobStoreAccessFactory implements an algorithm to return a {@link BlobStoreAccess} for a new blob. This could
 * for example based on the size of the blob. The BlobStoreAccess that will be returned must have been
 * registered with the repository, see {@link Repository#registerBlobStoreAccess(BlobStoreAccess)}.
 */
public interface BlobStoreAccessFactory {
    BlobStoreAccess get(Blob blob);
    Collection<BlobStoreAccess> getAll();

}
