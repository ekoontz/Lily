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
package org.lilyproject.repository.api;

public class VersionNotFoundException extends RepositoryException {

    private final Record record;

    public VersionNotFoundException(Record record) {
        this.record = record;
    }

    public Record getRecord() {
        return record;
    }

    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();
        message.append("Record <");
        message.append(record.getId());
        message.append("> ");
        Long version = record.getVersion();
        if (version != null) {
            message.append("<version:");
            message.append(version);
            message.append(">");
        }
        message.append("not found");
        return message.toString();
    }
}

