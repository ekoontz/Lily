package org.lilycms.rowlog.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class EmbededSubscriptionHandler extends AbstractSubscriptionHandler {
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private Future<?> future;
    
    public EmbededSubscriptionHandler(int subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog) {
        super(subscriptionId, messagesWorkQueue, rowLog);
    }
    
    public void start() {
        submitWorker();
    }

    protected void submitWorker() {
        future = executorService.submit(new Worker("Embeded"));
    }
    
    public void interrupt() {
        future.cancel(true);
        future = null;
    }
    
    @Override
    protected boolean processMessage(String context, RowLogMessage message) {
        return rowLog.getConsumer(subscriptionId).processMessage(message);
    }
}
