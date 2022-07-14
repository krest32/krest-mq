package com.krest.mq.core.server;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MQTCPServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MQTCPServer implements MQServer {
    NioEventLoopGroup bossGroup;
    NioEventLoopGroup workGroup;

    MQConfig mqConfig;

    private MQTCPServer() {
    }

    public MQTCPServer(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }

    @Override
    public void start() {
        bossGroup = new NioEventLoopGroup();
        workGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(mqConfig.getPort())
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // 客户端 -> 解码器
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());//解决粘包半包编码器
                        // 加入一个Decoder
                        ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                        ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());//解决粘包半包编码器
                        ch.pipeline().addLast(new ProtobufEncoder());
                        ch.pipeline().addLast(new MQTCPServerHandler());
                    }
                });

        try {
            ChannelFuture future = serverBootstrap.bind().sync();
            log.info("mq server start at : {} ", mqConfig.getPort());
            initData();
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

    private void initData() {
        System.out.println("初始化数据：");
        // 由于 channel 未能序列化，所以仅仅保存当前 queue的信息
        LocalCache.queueInfoMap = (ConcurrentHashMap<String, QueueInfo>)
                KrestFileHandler.readObject(CacheFileConfig.queueInfoFilePath);


        if (LocalCache.queueInfoMap == null) {
            LocalCache.queueInfoMap = new ConcurrentHashMap<>();
        }

        if (LocalCache.queueCtxListMap == null) {
            LocalCache.queueCtxListMap = new ConcurrentHashMap<>();
        }
        if (LocalCache.ctxQueueListMap == null) {
            LocalCache.ctxQueueListMap = new ConcurrentHashMap<>();
        }
        // 输出展示
        System.out.println(LocalCache.queueInfoMap);
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
