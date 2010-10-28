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
package org.lilyproject.indexer.model.api;

import java.util.Collection;

public interface IndexerModel {    
    Collection<IndexDefinition> getIndexes();

    /**
     * Gets the list of indexes, and registers a listener for future changes to the indexes. It guarantees
     * that the listener will receive events for all updates that happened after the returned snapshot
     * of the indexes.
     *
     * <p>In case you are familiar with ZooKeeper, note that the listener does not work like the watcher
     * in ZooKeeper: listeners are not one-time only.
     */
    Collection<IndexDefinition> getIndexes(IndexerModelListener listener);

    IndexDefinition getIndex(String name) throws IndexNotFoundException;

    boolean hasIndex(String name);

    void registerListener(IndexerModelListener listener);

    void unregisterListener(IndexerModelListener listener);
}
