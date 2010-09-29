package org.lilycms.rowlog.impl;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;

public abstract class AbstractSubscriptionHandler implements SubscriptionHandler {
    protected final RowLog rowLog;
    protected final String rowLogId;
    protected final String subscriptionId;
    protected final MessagesWorkQueue messagesWorkQueue;
    private Log log = LogFactory.getLog(getClass());
    
    public AbstractSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue, RowLog rowLog) {
        this.rowLog = rowLog;
        this.rowLogId = rowLog.getId();
        this.subscriptionId = subscriptionId;
        this.messagesWorkQueue = messagesWorkQueue;
    }
    
    protected abstract boolean processMessage(String context, RowLogMessage message);
    
    protected class Worker implements Callable<Object> {
        private final String context;

        public Worker(String context) {
            this.context = context;
        }
        
        public Object call() {
            while(!Thread.interrupted()) {
                RowLogMessage message;
                try {
                    message = messagesWorkQueue.take();
                } catch (InterruptedException e) {
                    break;
                }
                if (message != null) {
                    try {
                        byte[] lock = rowLog.lockMessage(message, subscriptionId);
                        if (lock != null) {
                            if (!rowLog.isMessageDone(message, subscriptionId) && !rowLog.isProblematic(message, subscriptionId)) {
                                if (processMessage(context, message)) {
                                    if(rowLog.messageDone(message, subscriptionId, lock)) {
                                    }
                                } else {
                                    rowLog.unlockMessage(message, subscriptionId, lock);
                                }
                            } else {
                                rowLog.unlockMessage(message, subscriptionId, lock);
                            }
                        } else {
                            System.out.println("Message <"+message+"> locked");
                        }
                    } catch (RowLogException e) {
                        log.warn(String.format("RowLogException occured while processing message %1$s by subscription %2$s of rowLog %3$s", message, subscriptionId, rowLogId), e);
                    } finally {
                        messagesWorkQueue.done(message);
                    }
                }
            }
            return null;
        }
    }
}
