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
package org.lilyproject.rowlog.api;

import java.util.List;

public interface SubscriptionsObserver {

    /**
     * Notifies the subscriptions have changed.
     *
     * <p>An observer will never be called from more than one thread concurrently, i.e. all
     * changes are reported sequentially.
     *
     * @param subscriptions the full list of current subscriptions.
     */
    void subscriptionsChanged(List<RowLogSubscription> subscriptions);
}
