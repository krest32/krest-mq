package com.krest.mq.core.consumer;

import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;
import com.krest.mq.core.utils.MQUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQConsumer {

    private String host;
    private int port;
    private String routerKey;

    MQEntity mqEntity;

    ChannelInactiveListener inactiveListener;
    private Bootstrap bootstrap;
    private Channel channel;
    EventLoopGroup workGroup;
    ChannelInboundHandlerAdapter handlerAdapter;

    /**
     * 构造方法
     */
    public MQConsumer(String host, int port, String routerKey, ChannelInboundHandlerAdapter handlerAdapter) {
        this.host = host;
        this.port = port;
        this.routerKey = routerKey;
        this.handlerAdapter = handlerAdapter;
        workGroup = new NioEventLoopGroup();
        mqEntity = new MQEntity("consumer", this.routerKey, ModuleType.CONSUMER);
        inactiveListener = () -> {
            log.info("connection with server is closed.");
            log.info("try to reconnect to the server.");
            channel = null;
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, mqEntity);
            }
            while (channel == null);
        };
    }


    /**
     * 构造方法
     */
    public MQConsumer(String host, int port, String routerKey) {
        this.host = host;
        this.port = port;
        this.routerKey = routerKey;
        workGroup = new NioEventLoopGroup();
        mqEntity = new MQEntity("consumer", this.routerKey, ModuleType.CONSUMER);
        inactiveListener = () -> {
            log.info("connection with server is closed.");
            log.info("try to reconnect to the server.");
            channel = null;
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, mqEntity);
            }
            while (channel == null);
        };
    }


    public void connect() {
        bootstrap = new Bootstrap();
        try {
            bootstrap.group(workGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
                            ch.pipeline().addLast(new ObjectEncoder());
                            ch.pipeline().addLast(new MQConsumerHandler(inactiveListener));
//                            ch.pipeline().addLast(handlerAdapter);
                        }
                    });
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, mqEntity);
            }
            while (channel == null);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
