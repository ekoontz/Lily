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
package org.lilyproject.tools.import_.core;

public enum ImportResultType {
    CREATED(true),
    UPDATED(true),
    CONFLICT(false),
    UP_TO_DATE(true),
    CANNOT_CREATE_EXISTS(false),
    CANNOT_UPDATE_DOES_NOT_EXIST(false);

    private boolean isSuccess;

    private ImportResultType(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    /**
     * Does this result represent a success?
     */
    public boolean isSuccess() {
        return isSuccess;
    }
}
