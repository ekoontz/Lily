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
 * An IO exception happened during a blob operation, the operation was retried but eventually still failed.
 */
public class RetriesExhaustedBlobException extends BlobException {
    private String operation;
    private int attempts;
    private long duration;

    public RetriesExhaustedBlobException(String operation, int attempts, long duration, Throwable cause) {
        super(cause);
        this.operation = operation;
        this.attempts = attempts;
        this.duration = duration;
    }

    @Override
    public String getMessage() {
        return "Attempted " + operation + " operation " + attempts + " times during " + duration + " ms without success.";
    }
}
