package org.lilycms.rowlog.impl;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;
import org.lilycms.rowlog.api.RowLogMessageListenerMapping;

public class LocalListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    
    public LocalListenersSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog, RowLogConfigurationManagerImpl rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
    }
    
    protected boolean processMessage(String listenerId, RowLogMessage message) {
        RowLogMessageListener listener = RowLogMessageListenerMapping.INSTANCE.get(subscriptionId);
        if (listener == null)
            return false;
        return listener.processMessage(message);
    }
    
    
}
