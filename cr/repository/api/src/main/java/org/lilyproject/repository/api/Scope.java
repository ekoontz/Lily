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
 * Enumeration of the version scopes to which a field can belong.
 *
 * <p>The scopes define versioning-related behavior of a {@link FieldType}. For more background information, please
 * refer to the Lily repository model documentation.
 */
public enum Scope {
    NON_VERSIONED, VERSIONED, VERSIONED_MUTABLE 
}