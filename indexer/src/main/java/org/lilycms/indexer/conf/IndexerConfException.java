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

/**
 * Thrown when there is an error in the user-provided configuration.
 */
public class IndexerConfException extends Exception {
    public IndexerConfException() {
        super();
    }

    public IndexerConfException(String message) {
        super(getMessage(message));
    }

    public IndexerConfException(String message, Throwable cause) {
        super(getMessage(message), cause);
    }

    public IndexerConfException(Throwable cause) {
        super(cause);
    }

    private static String getMessage(String message) {
        return "Indexer configuration: " + message;
    }
}
