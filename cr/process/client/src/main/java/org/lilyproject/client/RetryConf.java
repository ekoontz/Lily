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
package org.lilyproject.client;

/**
 * This is a configuration object for {@link LilyClient} for the retry behavior of operations that fail due to
 * IOExceptions or due to the unavailability of Lily servers.
 */
public class RetryConf {
    private boolean retryReads = true;
    private boolean retryUpdates = true;
    private boolean retryDeletes = true;
    private boolean retryCreate = true;
    private boolean retryCreateRiskDoubles = false;
    private int retryMaxTime = 30000;
    private int[] retryIntervals = new int[] {10, 10, 50, 50, 200, 300, 600};

    public boolean getRetryReads() {
        return retryReads;
    }

    public void setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
    }

    public boolean getRetryUpdates() {
        return retryUpdates;
    }

    public void setRetryUpdates(boolean retryUpdates) {
        this.retryUpdates = retryUpdates;
    }

    public boolean getRetryDeletes() {
        return retryDeletes;
    }

    /**
     * If you enable retryDeletes, it is best to ignore the RecordNotFoundException.
     */
    public void setRetryDeletes(boolean retryDeletes) {
        this.retryDeletes = retryDeletes;
    }

    public boolean getRetryCreate() {
        return retryCreate;
    }

    /**
     * The exact behavior of retryCreate depends on whether retryCreateRiskDoubles is also enabled. If it is disabled,
     * then creates are only retried if we are sure the request was not sent out yet, thus when the initial connection
     * could not be established.
     */
    public void setRetryCreate(boolean retryCreate) {
        this.retryCreate = retryCreate;
    }

    public boolean getRetryCreateRiskDoubles() {
        return retryCreateRiskDoubles;
    }

    /**
     * See retryCreate. If you enable this, you will likely want to ignore or handle the RecordExistsException.
     * The RecordExistsException can be because the record was created by someone else, or because the request
     * was submitted to the server in the previous try. If you use UUIDs, you can probably rule out the first case.
     * If you do not assign a record ID yourself, you will not get a RecordExistsException, but you might have
     * created more than one record without realizing. In summary, use this with care.
     */
    public void setRetryCreateRiskDoubles(boolean retryCreateRiskDoubles) {
        this.retryCreateRiskDoubles = retryCreateRiskDoubles;
    }

    public int getRetryMaxTime() {
        return retryMaxTime;
    }

    /**
     * The maximum time we can spent waiting in retry-cycles until we given up.
     */
    public void setRetryMaxTime(int retryMaxTime) {
        this.retryMaxTime = retryMaxTime;
    }

    public int[] getRetryIntervals() {
        return retryIntervals;
    }

    /**
     * The time to wait after each failed attempt. The first entry in this array is the time to wait after the
     * first attempt, the second entry the time to wait after the second attempt, and so on. The last entry
     * in this array will be repeated until {@link #getRetryMaxTime()} is reached.
     */
    public void setRetryIntervals(int[] retryIntervals) {
        this.retryIntervals = retryIntervals;
    }
}