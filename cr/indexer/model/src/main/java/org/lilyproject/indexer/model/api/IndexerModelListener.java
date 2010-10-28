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

/**
 * Reports changes to the {@link IndexerModel}.
 *
 * <p>The methods are called from within a ZooKeeper Watcher event callback, so be careful what
 * you do in the implementation (should be short-running + not wait for ZK events itself). 
 */
public interface IndexerModelListener {
    void process(IndexerModelEvent event);
}
