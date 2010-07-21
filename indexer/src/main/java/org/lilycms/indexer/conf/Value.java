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
package org.lilycms.indexer.conf;

import java.util.List;

import org.lilycms.repository.api.IdRecord;
import org.lilycms.repository.api.Repository;

public interface Value {
    /**
     * Evaluates this value for a given record & vtag.
     *
     * @return null if there is no value
     */
    List<String> eval(IdRecord record, Repository repository, String vtag);

    /**
     * Returns the field that is used from the record when evaluating this value. It is the value that is taken
     * from the current record, thus in the case of a dereference it is the first link field, not the field value
     * taken from the target document.
     */
    String getFieldDependency();

}
