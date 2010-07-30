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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
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
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageConsumer;
import org.lilycms.rowlog.api.RowLogProcessor;
import org.lilycms.rowlog.api.RowLogShard;
import org.lilycms.util.ArgumentValidator;

public class RowLogProcessorImpl implements RowLogProcessor {
    private volatile boolean stop = true;
    private final RowLog rowLog;
    private final RowLogShard shard;
    private Map<Integer, ConsumerThread> consumerThreads = new HashMap<Integer, ConsumerThread>();
    private Channel channel;
    private ChannelFactory channelFactory;
    private final String zkConnectString;
    
    public RowLogProcessorImpl(RowLog rowLog, RowLogShard shard, String zkConnectString) {
        this.zkConnectString = zkConnectString;
        ArgumentValidator.notNull(rowLog, "rowLog");
        ArgumentValidator.notNull(shard, "shard");
        this.rowLog = rowLog;
        this.shard = shard;
    }
    
    @Override
    protected synchronized void finalize() throws Throwable {
        stop();
        super.finalize();
    }
    
    public synchronized void start() {
        if (stop) {
            stop = false;
            for (RowLogMessageConsumer consumer : rowLog.getConsumers()) {
                startConsumerThread(consumer);
            }
            startConsumerNotifyListener();
            rowLog.setProcessor(this);
        }
    }

    public synchronized void consumerRegistered(RowLogMessageConsumer consumer) {
        startConsumerThread(consumer);
    }
    
    public synchronized void consumerUnregistered(RowLogMessageConsumer consumer) {
        stopConsumerThread(consumer.getId());
    }
    
    private synchronized void startConsumerThread(RowLogMessageConsumer consumer) {
        if (!stop) {
            if (consumerThreads.get(consumer.getId()) == null) {
                ConsumerThread consumerThread = new ConsumerThread(consumer);
                consumerThread.start();
                consumerThreads.put(consumer.getId(), consumerThread);
            }
        }
    }
    
    private synchronized void stopConsumerThread(int consumerId) {
        ConsumerThread consumerThread = consumerThreads.get(consumerId);
        consumerThread.interrupt();
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
        }
        consumerThreads.remove(consumerId);
    }

    public synchronized void stop() {
        stop = true;
        stopConsumerNotifyListener();
        rowLog.setProcessor(null);
        Collection<ConsumerThread> threads = consumerThreads.values();
        consumerThreads.clear();
        for (Thread thread : threads) {
            if (thread != null) {
                thread.interrupt();
            }
        }
        for (Thread thread : threads) {
            if (thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public synchronized boolean isRunning(int consumerId) {
        return consumerThreads.get(consumerId) != null;
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
                publishHost(hostName, port);
            } catch (UnknownHostException e) {
                // Don't listen to any wakeup events
                // Fallback on the default timeout behaviour
            }
            
        }
    }
    
    private void publishHost(String hostname, int port) {
        if (zkConnectString == null) return;
        
        String lilyPath = "/lily";
        String rowLogPath = lilyPath + "/rowLog";
        String rowLogIdPath = rowLogPath + "/" + rowLog.getId();
        String shardIdPath = rowLogIdPath + "/" + shard.getId();
        ZooKeeper zookeeper;
        final Semaphore semaphore = new Semaphore(0);
        try {
            zookeeper = new ZooKeeper(zkConnectString, 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    if (KeeperState.SyncConnected.equals(event.getState())) {
                        semaphore.release();
                    }
                }
            });
        } catch (IOException e) {
            return;
        }
        try { 
            semaphore.acquire();
            try {
                zookeeper.create(lilyPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            try {
                zookeeper.create(rowLogPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            try {
                zookeeper.create(rowLogIdPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            try {
                zookeeper.create(shardIdPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            
            zookeeper.setData(shardIdPath, Bytes.toBytes(hostname + ":" + port), -1);
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        } finally {
            try {
                if (zookeeper != null) {
                    zookeeper.close();
                }
            } catch (InterruptedException e) {
            }
        }
    }
    
    private void unPublishHost() {
        if (zkConnectString == null) return;
        
        String lilyPath = "/lily";
        String rowLogPath = lilyPath + "/rowLog";
        String rowLogIdPath = rowLogPath + "/" + rowLog.getId();
        String shardIdPath = rowLogIdPath + "/" + shard.getId();
        ZooKeeper zookeeper = null;
        final Semaphore semaphore = new Semaphore(0);
        try {
            zookeeper = new ZooKeeper(zkConnectString, 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    if (KeeperState.SyncConnected.equals(event.getState())) {
                        semaphore.release();
                    }
                }
            });
            semaphore.acquire(); // Wait for asynchronous zookeeper session establishment
            zookeeper.delete(shardIdPath, -1);
        } catch (IOException e) {
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        } finally {
            try {
                if (zookeeper != null) {
                    zookeeper.close();
                }
            } catch (InterruptedException e) {
            }
        }
    }
    
    private void stopConsumerNotifyListener() {
        unPublishHost();
        if (channel != null) {
            ChannelFuture channelFuture = channel.close();
            channelFuture.awaitUninterruptibly();
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
            for (ConsumerThread consumerThread : consumerThreads.values()) {
                consumerThread.wakeup();
            }
            e.getChannel().close();
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            // Ignore and rely on the automatic retries
        }
    }
    
    private class ConsumerThread extends Thread {
        private final RowLogMessageConsumer consumer;
        private long lastWakeup;
        private ProcessorMetrics metrics;

        public ConsumerThread(RowLogMessageConsumer consumer) {
            this.consumer = consumer;
            this.metrics = new ProcessorMetrics();
        }
        
        public synchronized void wakeup() {
            metrics.incWakeupCount();
            lastWakeup = System.currentTimeMillis();
            this.notify();
        }
        
        public void run() {
            if ((consumerThreads.get(consumer.getId()) == null)) return;
            
            while (!isInterrupted()) {
                try {
                    List<RowLogMessage> messages = shard.next(consumer.getId());
                    metrics.setScannedMessages(messages != null ? messages.size() : 0);
                    if (messages != null && !messages.isEmpty()) {
                        for (RowLogMessage message : messages) {
                            if (isInterrupted())
                                return;

                            metrics.incMessageCount();

                            byte[] lock = rowLog.lockMessage(message, consumer.getId());
                            if (lock != null) {
                                if (consumer.processMessage(message)) {
                                    rowLog.messageDone(message, consumer.getId(), lock);
                                    metrics.incSuccessCount();
                                } else {
                                    rowLog.unlockMessage(message, consumer.getId(), lock);
                                    metrics.incFailureCount();
                                }
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
                record = lilyContext.createRecord("rowLogProcessor." + consumer.getId());
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
