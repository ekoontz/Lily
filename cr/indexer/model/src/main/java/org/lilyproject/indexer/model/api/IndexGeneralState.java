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

public enum IndexGeneralState {
    /**
     * Indicates this index can be used by clients. This state has no meaning to the indexer itself.
     */
    ACTIVE,

    /**
     * Indicates this index should not be used by clients. This state has no meaning to the indexer itself,
     * for example the index can still be updating or rebuilding while in this state.
     */
    DISABLED,

    /**
     * Indicates a request towards the indexer master to drop this index.
     */
    DELETE_REQUESTED,

    /**
     * Indicates the delete request is being processed.
     */
    DELETING,

    /**
     * Indicates a delete request failed, set again to {@link #DELETE_REQUESTED} to retry.
     */
    DELETE_FAILED;

    public boolean isDeleteState() {
        return this == IndexGeneralState.DELETE_REQUESTED
                || this == IndexGeneralState.DELETING
                || this == IndexGeneralState.DELETE_FAILED;
    }
}
