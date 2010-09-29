package org.lilycms.rowlog.impl;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;

public class LocalListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    
    public LocalListenersSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog, RowLogConfigurationManagerImpl rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
    }
    
    protected boolean processMessage(String listenerId, RowLogMessage message) {
        RowLogMessageListener consumer = ListenerClassMapping.INSTANCE.getListener(subscriptionId);
        if (consumer == null)
            return false;
        return consumer.processMessage(message);
    }
    
    
}
