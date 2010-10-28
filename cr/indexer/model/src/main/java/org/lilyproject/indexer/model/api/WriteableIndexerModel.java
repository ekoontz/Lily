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

import org.apache.zookeeper.KeeperException;
import org.lilyproject.util.zookeeper.ZkLockException;

public interface WriteableIndexerModel extends IndexerModel {

    /**
     * Instantiates an IndexDefinition object, but does not register it yet, you should
     * do this using {@link #addIndex}.
     */
    IndexDefinition newIndex(String name);

    void addIndex(IndexDefinition index) throws IndexExistsException, IndexModelException, IndexValidityException;

    /**
     * Loads an index definition and returns it in a mutable way.
     *
     * <p>This differs from {@link #getIndex(String)} in that the returned index definition
     * is mutable (updateable) and it is also freshly loaded from storage.
     */
    IndexDefinition getMutableIndex(String name) throws InterruptedException, KeeperException, IndexNotFoundException;

    /**
     * Updates an index.
     *
     * <p>The update will only succeed if it was not modified since it was read. This situation can be avoided
     * by taking a lock on the index before reading it. In fact, you are obliged to do so, and to pass your lock,
     * of which it will be validated that it really is the owner of the index lock.
     */
    void updateIndex(final IndexDefinition index, String lock) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, ZkLockException, IndexUpdateException, IndexValidityException;

    /**
     * Internal index update method, <b>this method is only intended for internal Lily components</b>. It
     * is similar to the update method but bypasses some checks.
     */
    void updateIndexInternal(final IndexDefinition index) throws InterruptedException, KeeperException,
            IndexNotFoundException, IndexConcurrentModificationException, IndexValidityException;

    void deleteIndex(final String indexName) throws IndexModelException;

    /**
     * Takes a lock on this index.
     *
     * <p>Taking a lock can avoid concurrent modification exceptions when updating the index.
     *
     * <p>TODO: can/should clients use this lock for their own purposes?
     */
    String lockIndex(String indexName) throws ZkLockException, IndexNotFoundException, InterruptedException,
            KeeperException, IndexModelException;

    void unlockIndex(String lock) throws ZkLockException;

    void unlockIndex(String lock, boolean ignoreMissing) throws ZkLockException;

    /**
     * Internal index lock method, <b>this method is only intended for internal Lily components</b>.
     */
    String lockIndexInternal(String indexName, boolean checkDeleted) throws ZkLockException, IndexNotFoundException,
            InterruptedException, KeeperException, IndexModelException;
}
