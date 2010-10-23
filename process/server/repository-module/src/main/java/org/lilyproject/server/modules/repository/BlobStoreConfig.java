package org.lilyproject.server.modules.repository;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.lilyproject.repository.api.BlobStoreAccess;
import org.lilyproject.repository.api.BlobStoreAccessFactory;
import org.lilyproject.repository.impl.DFSBlobStoreAccess;
import org.lilyproject.repository.impl.HBaseBlobStoreAccess;
import org.lilyproject.repository.impl.InlineBlobStoreAccess;
import org.lilyproject.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilyproject.util.repo.DfsUri;

public class BlobStoreConfig {
    static BlobStoreAccessFactory get(URI dfsUri, Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(DfsUri.getBaseDfsUri(dfsUri), configuration);
        Path blobRootPath = new Path(DfsUri.getDfsPath(dfsUri));

        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(fs, blobRootPath);
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(configuration);
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess(); 
        SizeBasedBlobStoreAccessFactory factory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        factory.addBlobStoreAccess(5000, inlineBlobStoreAccess);
        factory.addBlobStoreAccess(200000, hbaseBlobStoreAccess);
        return factory;
    }
}
