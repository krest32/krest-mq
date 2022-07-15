package com.krest.mq.core.server;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.config.MQBuilderConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MQTCPServerHandler;
import io.netty.bootstrap.ServerBootstrap;
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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class MQTCPServer implements MQServer {
    NioEventLoopGroup bossGroup;
    NioEventLoopGroup workGroup;

    MQBuilderConfig mqConfig;

    private MQTCPServer() {
    }

    public MQTCPServer(MQBuilderConfig mqConfig) {
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

        if (null == LocalCache.queueInfoMap) {
            LocalCache.queueInfoMap = new ConcurrentHashMap<>();
        }

        LocalCache.queueMap.put(MQNormalConfig.defaultAckQueue, new LinkedBlockingDeque<>(100));
        // 输出展示
        System.out.println(LocalCache.queueInfoMap);
        Iterator<Map.Entry<String, QueueInfo>> iterator = LocalCache.queueInfoMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> entry = iterator.next();
            String queueName = entry.getKey();
            QueueInfo queueInfo = entry.getValue();
            try {
                if (!queueInfo.getType().equals(QueueType.TEMPORARY)) {
                    // 开始构建队列
                    BlockingDeque<MQMessage.MQEntity> curBlockQueue = new LinkedBlockingDeque<>();
                    List<String> queueJsonData = KrestFileHandler.readData(
                            CacheFileConfig.queueCacheDatePath + queueName, queueInfo.getOffset());

                    // 开始清洗数据
                    for (String jsonData : queueJsonData) {
                        MQMessage.MQEntity.Builder tempBuilder = MQMessage.MQEntity.newBuilder();
                        JsonFormat.parser().merge((String) JSONObject.parse(jsonData),
                                tempBuilder);
                        MQMessage.MQEntity mqEntity = tempBuilder.build();
                        if (Long.valueOf(queueInfo.getOffset())
                                .compareTo(Long.valueOf(mqEntity.getId())) < 0) {
                            System.out.println(mqEntity.getId());
                            curBlockQueue.offer(mqEntity);
                        }
                    }
                    LocalCache.queueMap.put(queueName, curBlockQueue);
                }
            } catch (InvalidProtocolBufferException e) {
                log.error(e.getMessage(), e);
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
