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

public class InvalidRecordException extends RepositoryException {

    private final Record record;
    private final String message;

    public InvalidRecordException(Record record, String message) {
        this.record = record;
        // TODO Auto-generated constructor stub
        this.message = message;
    }
    
    public Record getRecord() {
        return record;
    }
    
    @Override
    public String getMessage() {
        return "Record <"+record.getId()+">: " + message;
    }

}
