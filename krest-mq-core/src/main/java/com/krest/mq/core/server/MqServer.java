package com.krest.mq.core.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqServer {
    NioEventLoopGroup bossGroup;
    NioEventLoopGroup workGroup;
    boolean isPushMode = false;
    int port;


    private MqServer() {
    }

    public MqServer(int port) {
        this.port = port;
    }

    public MqServer(int port, boolean isPushMode) {
        this.port = port;
        this.isPushMode = isPushMode;
    }

    public void start() {
        bossGroup = new NioEventLoopGroup();
        workGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(port)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
                        ch.pipeline().addLast(new ObjectEncoder());
                        ch.pipeline().addLast(new MqServerHandler(isPushMode));
                    }
                });
        try {
            ChannelFuture future = serverBootstrap.bind().sync();
            log.info("mq server start at : {} ", port);
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (null != bossGroup) {
                this.bossGroup.shutdownGracefully();
            }
            if (null != workGroup) {
                this.workGroup.shutdownGracefully();
            }
        }
    }

    public void stop() {
        if (null != bossGroup) {
            bossGroup.shutdownGracefully();

        }
        if (null != workGroup) {
            workGroup.shutdownGracefully();
        }
    }
}
