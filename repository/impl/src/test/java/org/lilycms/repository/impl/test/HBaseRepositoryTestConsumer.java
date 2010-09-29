package org.lilycms.repository.impl.test;

import junit.framework.Assert;

import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;

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
