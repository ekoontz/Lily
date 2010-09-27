package org.lilycms.rowlog.impl;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class LocalListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    
    public LocalListenersSubscriptionHandler(int subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog, RowLogConfigurationManagerImpl rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
        
    }
    
    protected boolean processMessage(String listenerId, RowLogMessage message) {
        return rowLog.getConsumer(subscriptionId).processMessage(message);
    }
    
    
}
