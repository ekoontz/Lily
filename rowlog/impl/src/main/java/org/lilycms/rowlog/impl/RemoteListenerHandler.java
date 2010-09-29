package org.lilycms.rowlog.impl;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.lilycms.rowlog.api.RowLog;
import org.lilycms.rowlog.api.RowLogException;
import org.lilycms.rowlog.api.RowLogMessage;
import org.lilycms.rowlog.api.RowLogMessageListener;

public class RemoteListenerHandler {
    private final Log log = LogFactory.getLog(getClass());
    private final RowLogMessageListener consumer;
    private ServerBootstrap bootstrap;
    private final RowLog rowLog;
    private Channel channel;
    private RowLogConfigurationManagerImpl rowLogConfigurationManager;
    private String listenerId;
    private final Configuration configuration;
    private final String subscriptionId;

    public RemoteListenerHandler(RowLog rowLog, String subscriptionId, RowLogMessageListener consumer, Configuration configuration) throws RowLogException {
        this.rowLog = rowLog;
        this.subscriptionId = subscriptionId;
        this.consumer = consumer;
        this.configuration = configuration;
        bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("messageDecoder", new MessageDecoder());
                pipeline.addLast("messageHandler", new MessageHandler());
                pipeline.addLast("resultEncoder", new ResultEncoder());
                return pipeline;
            }
        });
    }
    
    public void start() throws RowLogException {
        rowLogConfigurationManager = new RowLogConfigurationManagerImpl(configuration);
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RowLogException("Failed to start remote listener", e);
        }
        String hostName = inetAddress.getHostName();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, 0);
        channel = bootstrap.bind(inetSocketAddress);
        int port = ((InetSocketAddress)channel.getLocalAddress()).getPort();
        listenerId = hostName + ":" + port;
        rowLogConfigurationManager.addListener(rowLog.getId(), subscriptionId, listenerId);
    }
    
    public void interrupt() {
        ChannelFuture future = channel.close();
        try {
            future.await();
        } catch (InterruptedException e) {
        }
        bootstrap.releaseExternalResources();
        try {
            rowLogConfigurationManager.removeListener(rowLog.getId(), subscriptionId, listenerId);
        } catch (RowLogException e) {
            // TODO log
            e.printStackTrace();
        } 
        try {
            rowLogConfigurationManager.stop();
        } catch (InterruptedException e) {
        }
    }
    
    private class MessageDecoder extends OneToOneDecoder {
        @Override
        protected RowLogMessage decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBufferInputStream inputStream = new ChannelBufferInputStream((ChannelBuffer)msg);
            int idLength = inputStream.readInt();
            byte[] id = new byte[idLength];
            inputStream.readFully(id, 0, idLength);
            int rowKeyLength = inputStream.readInt();
            byte[] rowKey = new byte[rowKeyLength];
            inputStream.readFully(rowKey, 0, rowKeyLength);
            long seqnr = inputStream.readLong();
            int dataLength = inputStream.readInt();
            byte[] data = null;
            if (dataLength > 0) {
                data = new byte[dataLength];
                inputStream.readFully(data, 0, dataLength);
            }
            inputStream.close();
            return new RowLogMessageImpl(id, rowKey, seqnr, data, rowLog);
        }
    }
    
    private class MessageHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            RowLogMessage message = (RowLogMessage)e.getMessage();
            boolean result = consumer.processMessage(message);
            writeResult(e.getChannel(), result);
        }

        private void writeResult(Channel channel, boolean result) throws InterruptedException {
            ChannelFuture future = channel.write(new Boolean(result));
            future.await();
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            log.info("Failed to process message, "+ e.getCause());
            writeResult(e.getChannel(), false);
        }
    }
    
    private class ResultEncoder extends OneToOneEncoder {
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBuffer channelBuffer = ChannelBuffers.buffer(1);
            ChannelBufferOutputStream outputStream = new ChannelBufferOutputStream(channelBuffer);
            outputStream.writeBoolean((Boolean)msg);
            outputStream.close();
            return channelBuffer;
        }
    }
}
