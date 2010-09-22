package org.lilycms.rowlog.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class EmbededSubscriptionHandler extends AbstractSubscriptionHandler {
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private ExecutorService futuresExecutorService = Executors.newCachedThreadPool();
    Map<Integer, Future<?>> futures = new HashMap<Integer, Future<?>>();
    private int numberOfWorkers ;
    private boolean stop = false;
    
    public EmbededSubscriptionHandler(int subscriptionId, int numberOfWorkers, MessagesWorkQueue messagesWorkQueue, RowLog rowLog) {
        super(subscriptionId, messagesWorkQueue, rowLog);
        this.numberOfWorkers = numberOfWorkers;
    }
    
    public void start() {
        for (int i = 0; i < numberOfWorkers; i++) {
            submitWorker(i);
        }
    }

    protected void submitWorker(int i) {
        Future<?> future = executorService.submit(new Worker(Integer.toString(i)));
        futures.put(i, future);
        futuresExecutorService.submit(new Resubmitter(future, i));
    }
    
    public void interrupt() {
        stop = true;
        for (Future<?> future : futures.values()) {
            if (future != null)
                future.cancel(true);
        }
    }
    
    @Override
    protected boolean processMessage(String context, RowLogMessage message) {
        return rowLog.getConsumer(subscriptionId).processMessage(message);
    }
    
    private class Resubmitter implements Callable<Object> {
        private final Future<?> future;
        private final int i;

        public Resubmitter(Future<?> future, int i) {
            this.future = future;
            this.i = i;
        }
        
        public Object call() {
            try {
                future.get();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // Stop 
                futures.remove(i);
                return null;
            } 
            futures.remove(i);
            if (!stop)
                submitWorker(i);
            return null;
        }
    }
}
