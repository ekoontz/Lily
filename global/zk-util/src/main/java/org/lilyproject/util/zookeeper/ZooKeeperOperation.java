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
package org.lilyproject.util.zookeeper;

import org.apache.zookeeper.KeeperException;


// Disclaimer: this interface was copied from ZooKeeper's lock recipe and slightly altered.

/**
 * A callback object which can be used for implementing retry-able operations, used by
 * {@link ZooKeeperItf#retryOperation}.
 *
 */
public interface ZooKeeperOperation<T> {
    /**
     * Performs the operation - which may be involved multiple times if the connection
     * to ZooKeeper closes during this operation
     *
     */
    public T execute() throws KeeperException, InterruptedException;
}
