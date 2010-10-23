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

/**
 *
 */
public class FieldTypeNotFoundException extends RepositoryException {

    private final String id;
    private final QName name;

    public FieldTypeNotFoundException(String id) {
        this.id = id;
        this.name = null;
    }
    
    public FieldTypeNotFoundException(QName name) {
        this.id = null;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public QName getName() {
        return name;
    }

    @Override
    public String getMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("FieldType <");
        stringBuilder.append(id != null ? id : name);
        stringBuilder.append("> ");
        stringBuilder.append("could not be found.");
        return stringBuilder.toString();
    }
}
