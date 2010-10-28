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

import java.util.Comparator;

public class IndexDefinitionNameComparator implements Comparator<IndexDefinition> {
    public final static IndexDefinitionNameComparator INSTANCE = new IndexDefinitionNameComparator();

    public int compare(IndexDefinition o1, IndexDefinition o2) {
        return o1.getName().compareTo(o2.getName());
    }
}
