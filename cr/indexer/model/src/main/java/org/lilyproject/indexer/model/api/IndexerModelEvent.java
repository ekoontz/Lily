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

public class IndexerModelEvent {
    private IndexerModelEventType type;
    private String indexName;

    public IndexerModelEvent(IndexerModelEventType type, String indexName) {
        this.type = type;
        this.indexName = indexName;
    }

    public IndexerModelEventType getType() {
        return type;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + type.hashCode();
        result = prime * result + indexName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IndexerModelEvent other = (IndexerModelEvent) obj;
        return other.type == type && other.indexName.equals(indexName);
    }

    @Override
    public String toString() {
        return "Indexer model event [type = " + type + ", index = " + indexName + "]";
    }
}
