/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilycms.rowlog.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.lilycms.rowlog.api.*;

public class RowLogProcessorImpl implements RowLogProcessor, SubscriptionsObserver {
    private volatile boolean stop = true;
    private final RowLog rowLog;
    private final RowLogShard shard;
    private final Map<String, SubscriptionThread> subscriptionThreads = Collections.synchronizedMap(new HashMap<String, SubscriptionThread>());
    private Channel channel;
    private ChannelFactory channelFactory;
    private RowLogConfigurationManager rowLogConfigurationManager;
    private Log log = LogFactory.getLog(getClass());
    
    public RowLogProcessorImpl(RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) throws RowLogException {
        this.rowLog = rowLog;
        this.rowLogConfigurationManager = rowLogConfigurationManager;
        this.shard = rowLog.getShards().get(0); // TODO: For now we only work with one shard
    }

    public RowLog getRowLog() {
        return rowLog;
    }

    @Override
    protected synchronized void finalize() throws Throwable {
        stop();
        super.finalize();
    }
    
    public synchronized void start() {
        if (stop) {
            stop = false;
            rowLogConfigurationManager.addSubscriptionsObserver(rowLog.getId(), this);
            startConsumerNotifyListener();
        }
    }

    //  synchronized because we do not want to run this concurrently with the start/stop methods
    public synchronized void subscriptionsChanged(List<RowLogSubscription> newSubscriptions) {
        synchronized (subscriptionThreads) {
            if (!stop) {
                List<String> newSubscriptionIds = new ArrayList<String>();
                for (RowLogSubscription newSubscription : newSubscriptions) {
                    newSubscriptionIds.add(newSubscription.getId());
                    if (!subscriptionThreads.containsKey(newSubscription.getId())) {
                        SubscriptionThread subscriptionThread = startSubscriptionThread(newSubscription);
                        subscriptionThreads.put(newSubscription.getId(), subscriptionThread);
                    }
                }
                Iterator<String> iterator = subscriptionThreads.keySet().iterator();
                while (iterator.hasNext()) {
                    String subscriptionId = iterator.next();
                    if (!newSubscriptionIds.contains(subscriptionId)) {
                        stopSubscriptionThread(subscriptionId);
                        iterator.remove();
                    }
                }
            }
        }
    }

    private SubscriptionThread startSubscriptionThread(RowLogSubscription subscription) {
        SubscriptionThread subscriptionThread = new SubscriptionThread(subscription);
        subscriptionThread.start();
        return subscriptionThread;
    }
    
    private void stopSubscriptionThread(String subscriptionId) {
        SubscriptionThread subscriptionThread = subscriptionThreads.get(subscriptionId);
        subscriptionThread.interrupt();
        try {
            subscriptionThread.join();
        } catch (InterruptedException e) {
        }
    }

    public synchronized void stop() {
        stop = true;
        rowLogConfigurationManager.removeSubscriptionsObserver(rowLog.getId(), this);
        stopConsumerNotifyListener();
        Collection<SubscriptionThread> threadsToStop;
        synchronized (subscriptionThreads) {
            threadsToStop = new ArrayList<SubscriptionThread>(subscriptionThreads.values());
            subscriptionThreads.clear();
        }
        for (Thread thread : threadsToStop) {
            if (thread != null) {
                thread.interrupt();
            }
        }
        for (Thread thread : threadsToStop) {
            if (thread != null) {
                try {
                    if (thread.isAlive()) {
                        thread.join();
                    }
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public boolean isRunning(int consumerId) {
        return subscriptionThreads.get(consumerId) != null;
    }
    
    private void startConsumerNotifyListener() {
        if (channel == null) {
            if (channelFactory == null) { 
                channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            }
            ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
            
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline() throws Exception {
                    return Channels.pipeline(new NotifyDecoder(), new ConsumersNotifyHandler());
                }
            });
            
            bootstrap.setOption("child.tcpNoDelay", true);
            bootstrap.setOption("child.keepAlive", true);
            
            String hostName = null;
            try {
                InetAddress inetAddress = InetAddress.getLocalHost();
                hostName = inetAddress.getHostName();
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, 0);
                channel = bootstrap.bind(inetSocketAddress);
                int port = ((InetSocketAddress)channel.getLocalAddress()).getPort();
                rowLogConfigurationManager.publishProcessorHost(hostName, port, rowLog.getId(), shard.getId());
            } catch (UnknownHostException e) {
                // Don't listen to any wakeup events
                // Fallback on the default timeout behaviour
                log.warn("Did not start the server for waking up the processor for row log " + rowLog.getId());
            }
        }
    }
    
    private void stopConsumerNotifyListener() {
        rowLogConfigurationManager.unPublishProcessorHost(rowLog.getId(), shard.getId());
        if (channel != null) {
            ChannelFuture channelFuture = channel.close();
            try {
                channelFuture.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            channel = null;
        }
        if (channelFactory != null) {
            channelFactory.releaseExternalResources();
            channelFactory = null;
        }
    }

    private class NotifyDecoder extends FrameDecoder {
        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            if (buffer.readableBytes() < 1) {
                return null;
            }
            
            return buffer.readBytes(1);
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            // Ignore and rely on the automatic retries
        }
    }
    
    private class ConsumersNotifyHandler extends SimpleChannelHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            ChannelBuffer buffer = (ChannelBuffer)e.getMessage();
            byte notifyByte = buffer.readByte(); // Does not contain any usefull information currently
            Collection<SubscriptionThread> threadsToWakeup;
            synchronized (subscriptionThreads) {
                threadsToWakeup = new HashSet<SubscriptionThread>(subscriptionThreads.values());
            }
            for (SubscriptionThread consumerThread : threadsToWakeup) {
                consumerThread.wakeup();
            }
            e.getChannel().close();
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            // Ignore and rely on the automatic retries
        }
    }
    
    private class SubscriptionThread extends Thread {
        private long lastWakeup;
        private ProcessorMetrics metrics;
        private volatile boolean stopRequested = false;
        private MessagesWorkQueue messagesWorkQueue = new MessagesWorkQueue();
        private SubscriptionHandler subscriptionHandler;
        private String subscriptionId;

        public SubscriptionThread(RowLogSubscription subscription) {
            this.subscriptionId = subscription.getId();
            this.metrics = new ProcessorMetrics();
            switch (subscription.getType()) {
            case VM:
                subscriptionHandler = new LocalListenersSubscriptionHandler(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
                break;
                
            case Netty:
                subscriptionHandler = new RemoteListenersSubscriptionHandler(subscriptionId,  messagesWorkQueue, rowLog, rowLogConfigurationManager);

            default:
                break;
            }
        }
        
        public synchronized void wakeup() {
            metrics.incWakeupCount();
            lastWakeup = System.currentTimeMillis();
            this.notify();
        }
        
        @Override
        public synchronized void start() {
            stopRequested = false;
            subscriptionHandler.start();
            super.start();
        }
        
        @Override
        public void interrupt() {
            stopRequested = true;
            subscriptionHandler.interrupt();
            super.interrupt();
        }
                
        public void run() {
            while (!isInterrupted() && !stopRequested) {
                try {
                    List<RowLogMessage> messages = shard.next(subscriptionId);
                    metrics.setScannedMessages(messages != null ? messages.size() : 0);
                    if (messages != null && !messages.isEmpty()) {
                        for (RowLogMessage message : messages) {
                            if (isInterrupted())
                                return;

                            try {
                                if (!rowLog.isMessageDone(message, subscriptionId) && !rowLog.isProblematic(message, subscriptionId)) {
                                    messagesWorkQueue.offer(message);
                                } 
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                    } else {
                        try {
                            long timeout = 5000;
                            long now = System.currentTimeMillis();
                            if (lastWakeup + timeout < now) {
                                synchronized (this) {
                                    wait(timeout);
                                }
                            }
                        } catch (InterruptedException e) {
                            // if we are interrupted, we stop working
                            return;
                        }
                    }
                } catch (RowLogException e) {
                    // The message will be retried later
                }
            }
        }

        private class ProcessorMetrics implements Updater {
            private int scanCount = 0;
            private long scannedMessageCount = 0;
            private int messageCount = 0;
            private int successCount = 0;
            private int failureCount = 0;
            private int wakeupCount = 0;
            private MetricsRecord record;

            public ProcessorMetrics() {
                MetricsContext lilyContext = MetricsUtil.getContext("lily");
                record = lilyContext.createRecord("rowLogProcessor." + subscriptionId);
                lilyContext.registerUpdater(this);
            }

            public synchronized void doUpdates(MetricsContext unused) {
                record.setMetric("scanCount", scanCount);
                record.setMetric("messagesPerScan", scanCount > 0 ? scannedMessageCount / scanCount : 0f);
                record.setMetric("messageCount", messageCount);
                record.setMetric("successCount", successCount);
                record.setMetric("failureCount", failureCount);
                record.setMetric("wakeupCount", wakeupCount);
                record.update();

                scanCount = 0;
                scannedMessageCount = 0;
                messageCount = 0;
                successCount = 0;
                failureCount = 0;
                wakeupCount = 0;
            }

            synchronized void setScannedMessages(int read) {
                scanCount++;
                scannedMessageCount += read;
            }

            synchronized void incMessageCount() {
                messageCount++;
            }

            synchronized void incSuccessCount() {
                successCount++;
            }

            synchronized void incFailureCount() {
                failureCount++;
            }

            synchronized void incWakeupCount() {
                wakeupCount++;
            }
        }
    }

    
}
