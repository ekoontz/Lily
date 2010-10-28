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
package org.lilyproject.repository.impl.test;

import junit.framework.Assert;

import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;

public class HBaseRepositoryTestConsumer implements RowLogMessageListener {
    private static RowLogMessage message = null;
    private static RowLogMessage processedMessage = null;
    
    public static void reset() {
        message = null;
        processedMessage = null;
    }
    
    public boolean processMessage(RowLogMessage message) {
        if (HBaseRepositoryTestConsumer.message == null) {
            HBaseRepositoryTestConsumer.message = message;
            return false;
        } else {
            if (processedMessage == null) {
                Assert.assertEquals(HBaseRepositoryTestConsumer.message, message);
                processedMessage = message;
                return true;
            } else {
                Assert.assertFalse(processedMessage.equals(message));
                return true;
            }
        }
    }
    
}
