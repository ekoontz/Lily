package org.lilycms.rowlog.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class LocalListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    public LocalListenersSubscriptionHandler(int subscriptionId, int workerCount, BlockingQueue<RowLogMessage> messageQueue, RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        super(subscriptionId, messageQueue, rowLog, rowLogConfigurationManager);
        executorService = Executors.newFixedThreadPool(workerCount);
    }
    
    protected boolean processMessage(String listenerId, RowLogMessage message) {
        return rowLog.getConsumer(subscriptionId).processMessage(message);
    }
    
    
}
