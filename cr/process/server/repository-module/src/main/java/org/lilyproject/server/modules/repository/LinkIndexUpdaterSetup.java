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
package org.lilyproject.server.modules.repository;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.hbaseindex.IndexManager;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.linkindex.LinkIndex;
import org.lilyproject.linkindex.LinkIndexUpdater;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.rowlog.api.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * Installs the row log listener for the link index updater subscription.
 */
public class LinkIndexUpdaterSetup {
    private final Repository repository;
    private final Configuration hbaseConf;

    public LinkIndexUpdaterSetup(Repository repository, Configuration hbaseConf) {
        this.repository = repository;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void start() throws InterruptedException, KeeperException, IOException, RowLogException, IndexNotFoundException {
        // The registration of the subscription for the link index happens in the rowlog module,
        // to be sure it is already installed before the repository is started.

        IndexManager.createIndexMetaTableIfNotExists(hbaseConf);
        IndexManager indexManager = new IndexManager(hbaseConf);

        LinkIndex.createIndexes(indexManager);
        LinkIndex linkIndex = new LinkIndex(indexManager, repository);

        LinkIndexUpdater linkIndexUpdater = new LinkIndexUpdater(repository, linkIndex);

        RowLogMessageListenerMapping.INSTANCE.put("LinkIndexUpdater", linkIndexUpdater);
    }

    @PreDestroy
    public void stop() {
        RowLogMessageListenerMapping.INSTANCE.remove("LinkIndexUpdater");
    }
}
