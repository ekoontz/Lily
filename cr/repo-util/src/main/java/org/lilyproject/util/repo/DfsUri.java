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
package org.lilyproject.util.repo;

import java.net.URI;
import java.net.URISyntaxException;

public class DfsUri {
    public static URI getBaseDfsUri(URI dfsUri) {
        // It is not strictly necessary to construct this uri without path, since Hadoop ignores the
        // path anyway, but the supplied uri is used as cache key, so seems better to do this.
        URI dfsBaseUri;
        try {
            dfsBaseUri = new URI(dfsUri.getScheme() + "://" + dfsUri.getAuthority());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return dfsBaseUri;
    }

    public static String getDfsPath(URI dfsUri) {
        if (dfsUri.getPath() == null || dfsUri.getPath().equals("")) {
            throw new RuntimeException("Please specify a path in blob DFS uri: " + dfsUri);
        }
        return dfsUri.getPath();
    }
}
