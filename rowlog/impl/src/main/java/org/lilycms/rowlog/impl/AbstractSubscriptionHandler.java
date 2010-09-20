package org.lilycms.rowlog.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public abstract class AbstractSubscriptionHandler implements SubscriptionHandler {
    protected final RowLog rowLog;
    protected final String rowLogId;
    protected final int subscriptionId;
    protected final BlockingQueue<RowLogMessage> messageQueue;
    
    public AbstractSubscriptionHandler(int subscriptionId, BlockingQueue<RowLogMessage> messageQueue, RowLog rowLog) {
        this.rowLog = rowLog;
        this.rowLogId = rowLog.getId();
        this.subscriptionId = subscriptionId;
        this.messageQueue = messageQueue;
    }
    
    protected abstract boolean processMessage(String context, RowLogMessage message);
    
    protected class Worker implements Callable<Object> {
        private final String context;

        public Worker(String context) {
            this.context = context;
        }
        
        public Object call() throws Exception {
            RowLogMessage message = messageQueue.poll(1, TimeUnit.SECONDS);
            if (message != null) {
//                metrics.incMessageCount();
                byte[] lock = rowLog.lockMessage(message, subscriptionId);
                if (lock != null) {
                    if (!rowLog.isMessageDone(message, subscriptionId) && !rowLog.isProblematic(message, subscriptionId)) {
                        if (processMessage(context, message)) {
                            if(rowLog.messageDone(message, subscriptionId, lock)) {
    //                        metrics.incSuccessCount();
                            }
                        } else {
                            rowLog.unlockMessage(message, subscriptionId, lock);
    //                        metrics.incFailureCount();
                        }
                    } else {
                        rowLog.unlockMessage(message, subscriptionId, lock);
                    }
                } 
            }
            return null;
        }
    }
}
