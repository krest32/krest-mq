package com.krest.mq.core.client;

import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.MQUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQClient {

    private String host;
    private int port;

    ChannelInactiveListener inactiveListener;
    private Bootstrap bootstrap;
    private Channel channel;
    MQMessage.MQEntity request;
    EventLoopGroup workGroup = new NioEventLoopGroup();


    private MQClient() {
    }

    public ChannelInactiveListener getInactiveListener() {
        return inactiveListener;
    }

    /**
     * 构造方法
     */
    public MQClient(String host, int port, MQMessage.MQEntity request) {
        this.host = host;
        this.port = port;
        this.request = request;
        inactiveListener = () -> {
            log.info("connection with server is closed.");
            log.info("try to reconnect to the server.");
            channel = null;
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, request);
            }
            while (channel == null);
        };
    }


    public void sendMsg(MQMessage.MQEntity request) throws InterruptedException {
        if (channel == null) {
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, request);
            }
            while (channel == null);
        }
        channel.writeAndFlush(request).sync();
    }

    public void connect(ChannelInboundHandlerAdapter handlerAdapter) {
        bootstrap = new Bootstrap();
        try {
            bootstrap.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            // 客户端 -> 解码器
                            ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                            ch.pipeline().addLast(new ProtobufEncoder());
                            // ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
                            // ch.pipeline().addLast(new ObjectEncoder());
                            ch.pipeline().addLast(handlerAdapter);
                        }
                    });
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, request);
            }
            while (channel == null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void stop() {
        if (null != this.workGroup) {
            this.workGroup.shutdownGracefully();
        }
    }
}
