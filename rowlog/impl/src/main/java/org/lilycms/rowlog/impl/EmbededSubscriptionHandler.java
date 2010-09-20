package org.lilycms.rowlog.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogMessage;

public class EmbededSubscriptionHandler extends AbstractSubscriptionHandler {
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private ExecutorService futuresExecutorService = Executors.newCachedThreadPool();
    List<Future<?>> futures = new ArrayList<Future<?>>();
    private int numberOfWorkers ;
    private boolean stop = false;
    
    public EmbededSubscriptionHandler(int subscriptionId, int numberOfWorkers, BlockingQueue<RowLogMessage> messageQueue, RowLog rowLog) {
        super(subscriptionId, messageQueue, rowLog);
        this.numberOfWorkers = numberOfWorkers;
    }
    
    public void start() {
        for (int i = 0; i < numberOfWorkers; i++) {
            submitWorker(i);
        }
    }

    protected void submitWorker(int i) {
        Future<?> future = executorService.submit(new Worker(Integer.toString(i)));
        futures.add(future);
        futuresExecutorService.submit(new Resubmitter(future, i));
    }
    
    public void interrupt() {
        stop = true;
        for (Future<?> future : futures) {
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
        
        public Object call() throws Exception {
            future.get(6, TimeUnit.SECONDS);
            if (!future.isDone() && !future.isCancelled())
                future.cancel(true);
            if (!stop)
                submitWorker(i);
            return null;
        }
    }
}
