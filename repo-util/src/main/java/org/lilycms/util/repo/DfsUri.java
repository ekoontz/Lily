package org.lilycms.util.repo;

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
