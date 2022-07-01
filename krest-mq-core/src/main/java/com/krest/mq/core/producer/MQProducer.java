package com.krest.mq.core.producer;

import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;
import com.krest.mq.core.utils.MQUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MQProducer {

    private String host;
    private int port;

    ChannelInactiveListener inactiveListener;
    private Bootstrap bootstrap;
    private Channel channel;
    MQEntity mqEntity;
    EventLoopGroup workGroup = new NioEventLoopGroup();

    /**
     * 构造方法
     */
    public MQProducer(String host, int port) {
        this.host = host;
        this.port = port;
        this.mqEntity = new MQEntity("producer", "default", ModuleType.PRODUCER);
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


    public void sendMsg(MQEntity entity) throws InterruptedException {
        if (channel == null) {
            do {
                channel = MQUtils.tryConnect(bootstrap, host, port, mqEntity);
            }
            while (channel == null);
        }
        channel.writeAndFlush(entity).sync();
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
                            ch.pipeline().addLast(new MQProducerHandler(inactiveListener));
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


    public void stop() {
        if (null != this.workGroup) {
            this.workGroup.shutdownGracefully();
        }
    }

}
