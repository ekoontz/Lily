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
package org.lilyproject.rest;

public class ResourceException extends RuntimeException {
    private int status;
    private String message;

    public ResourceException(String message, int status) {
        super(message);
        this.status = status;
        this.message = message;
    }

    public ResourceException(String message, Throwable cause, int status) {
        super(message, cause);
        this.status = status;
        this.message = message;
    }

    public ResourceException(Throwable cause, int status) {
        super(cause);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    /**
     * Gets the message specifically set on this exception. This is different
     * from {@link #getMessage()} which might inherit the message from a nested
     * throwable or so.
     */
    public String getSpecificMessage() {
        return message;
    }
}
