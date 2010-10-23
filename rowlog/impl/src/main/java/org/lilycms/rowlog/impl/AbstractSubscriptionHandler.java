package org.lilycms.rowlog.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.util.Logs;

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
    
    protected abstract boolean processMessage(String context, RowLogMessage message) throws InterruptedException;
    
    protected class Worker implements Runnable {
        private final String subscriptionId;
        private final String context;
        private Thread thread;
        private volatile boolean stop; // do not rely only on Thread.interrupt since some libraries eat interruptions

        public Worker(String subscriptionId, String context) {
            this.subscriptionId = subscriptionId;
            this.context = context;
        }

        public void start() {
            thread = new Thread(this, "Handler: subscription " + subscriptionId + ", listener " + context);
            thread.start();
        }

        public void stop() throws InterruptedException {
            stop = true;
            thread.interrupt();
            Logs.logThreadJoin(thread);
            thread.join();
        }

        public void run() {
            while(!stop && !Thread.interrupted()) {
                RowLogMessage message;
                try {
                    message = messagesWorkQueue.take();
                    if (message != null) {
                        try {
                            byte[] lock = rowLog.lockMessage(message, subscriptionId);
                            if (lock != null) {
                                if (!rowLog.isMessageAvailable(message, subscriptionId) || rowLog.isProblematic(message, subscriptionId)) {
                                    rowLog.unlockMessage(message, subscriptionId, false, lock);
                                } else {
                                    if (processMessage(context, message)) {
                                        rowLog.messageDone(message, subscriptionId, lock);
                                    } else {
                                        rowLog.unlockMessage(message, subscriptionId, true, lock);
                                    }
                                }
                            }
                        } catch (InterruptedException e) {
                            break;                            
                        } catch (Throwable e) {
                            log.warn(String.format("RowLogException occurred while processing message %1$s by subscription %2$s of rowLog %3$s", message, subscriptionId, rowLogId), e);
                        } finally {
                            messagesWorkQueue.done(message);
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
