package org.lilycms.rowlog.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class RowLogProcessorNotifier {
    
    private final String zkConnectString;
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private String[] processorHostAndPort;

    public RowLogProcessorNotifier(String zkConnectString) {
        this.zkConnectString = zkConnectString;
    }
    
    protected void notifyProcessor(String rowLogId, String shardId) {
        Channel channel = getProcessorChannel(rowLogId, shardId);
        if ((channel != null) && (channel.isConnected())) { 
            ChannelBuffer channelBuffer = ChannelBuffers.buffer(1);
            channelBuffer.writeByte(1);
            ChannelFuture writeFuture = channel.write(channelBuffer);
            writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }
    
    protected void close() {
        processorHostAndPort = null;
        if (channelFactory != null) {
            channelFactory.releaseExternalResources();
            channelFactory = null;
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
    
    private Channel getProcessorChannel(String rowLogId, String shardId) {
        if (processorHostAndPort == null) {
            readProcessorHostAndPort(rowLogId, shardId);
        }
        if (processorHostAndPort != null) {
            initBootstrap();
            ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(processorHostAndPort[0], Integer.valueOf(processorHostAndPort[1])));
            try {
                if (connectFuture.await(1000)) {
                    if (connectFuture.isSuccess()) {
                        return connectFuture.getChannel();
                    } else {
                        processorHostAndPort = null; // Re-read from Zookeeper next time
                        return null;
                    }
                    
                }
            } catch (InterruptedException e) {
            }
            processorHostAndPort = null; // Re-read from Zookeeper next time
        }
        return null;
    }
    
    private void readProcessorHostAndPort(String rowLogId, String shardId) {
        ZooKeeper zookeeper = connectZookeeper();
        if (zookeeper != null) {
            String data = null;
            try {
                String shardIdPath = "/lily/rowLog/" + rowLogId + "/" + shardId; 
                if (zookeeper.exists(shardIdPath, false) != null) {
                    data = Bytes.toString(zookeeper.getData(shardIdPath, false, new Stat()));
                } 
            } catch (KeeperException e) {
            } catch (InterruptedException e) {
            } 
            
            if (data != null) {
                processorHostAndPort = data.split(":");
            }
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
            } 
        }
    }

    private ZooKeeper connectZookeeper() {
        ZooKeeper zooKeeper;
        final Semaphore semaphore = new Semaphore(0);
        try {
            zooKeeper = new ZooKeeper(zkConnectString, 5000, new Watcher() {
                public void process(WatchedEvent event) {
                    
                    KeeperState keeperState = event.getState();
                    if (KeeperState.SyncConnected.equals(keeperState)) {
                        semaphore.release();
                    }
                }
            });
        } catch (IOException e) {
            return null;
        }
        try {
            semaphore.acquire(); // Wait for asynchronous zookeeper session establishment
        } catch (InterruptedException e) {
            return null;
        }
        return zooKeeper;
    }

    private void initBootstrap() {
        if (bootstrap == null) {
            if (channelFactory == null) {
                channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            }
            bootstrap = new ClientBootstrap(channelFactory);
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline() {
                    return Channels.pipeline(new SimpleChannelHandler() {
                        
                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
                            // The connection will not be successful. A new host and port will be read from Zookeeper next time.
                        }
                    });
                }
            });
            bootstrap.setOption("tcpNoDelay", true);
            bootstrap.setOption("keepAlive", true);
        }
    }
}
