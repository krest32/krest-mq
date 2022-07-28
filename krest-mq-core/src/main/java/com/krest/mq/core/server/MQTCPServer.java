package com.krest.mq.core.server;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MqTcpServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class MQTCPServer {
    NioEventLoopGroup bossGroup;
    NioEventLoopGroup workGroup;
    Channel channel;
    // 当前会启动 tcp 与 udp 两个 server
    Integer port;

    private MQTCPServer() {
    }

    public MQTCPServer(Integer port) {
        this.port = port;
    }


    public void start() {
        bossGroup = new NioEventLoopGroup();
        workGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(this.port)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // 客户端 -> 解码器
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());//解决粘包半包编码器
                        // 加入一个Decoder
                        ch.pipeline().addLast(new ProtobufDecoder(MQMessage.MQEntity.getDefaultInstance()));
                        ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());//解决粘包半包编码器
                        ch.pipeline().addLast(new ProtobufEncoder());
                        ch.pipeline().addLast(new MqTcpServerHandler());
                    }
                });
        try {
            ChannelFuture future = serverBootstrap.bind().sync();
            this.channel = future.channel();
            // 初始化数据要在之前
            initData();
            log.info("mq tcp server start at : {} ", this.port);
            // 线程会阻塞在这么
            this.channel.closeFuture().sync();
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
        log.info("开始初始化数据...");
        // 由于 channel 未能序列化，所以仅仅保存当前 queue的信息
        BrokerLocalCache.queueInfoMap = (ConcurrentHashMap<String, QueueInfo>)
                KrestFileHandler.readObject(CacheFileConfig.queueInfoFilePath);

        if (null == BrokerLocalCache.queueInfoMap) {
            BrokerLocalCache.queueInfoMap = new ConcurrentHashMap<>();
        }

        // 还原队列信息
        recoverQueueData();
        log.info("queue info map :{} ", BrokerLocalCache.queueInfoMap);
        log.info("初始化数据完成");
    }


    private void recoverQueueData() {
        // 新建一个默认的 ack queue
        BrokerLocalCache.queueMap.put(MQNormalConfig.defaultAckQueue, new LinkedBlockingDeque<>());
        Iterator<Map.Entry<String, QueueInfo>> iterator = BrokerLocalCache.queueInfoMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> entry = iterator.next();
            String queueName = entry.getKey();
            QueueInfo queueInfo = entry.getValue();
            try {
                // 如果是普通持久化队列
                if (queueInfo.getType().equals(QueueType.PERMANENT)) {
                    recoverNormalQueue(queueName, queueInfo);
                    // 回复延时队列数据
                } else if (queueInfo.getType().equals(QueueType.DELAY)) {
                    recoverDelayQueue(queueName, queueInfo);
                } else {
                    log.error("临时队列 -> ：{}", queueName);
                    BlockingDeque<MQMessage.MQEntity> curBlockQueue = new LinkedBlockingDeque<>();
                    BrokerLocalCache.queueInfoMap.get(queueName).setAmount(curBlockQueue.size());
                    BrokerLocalCache.queueMap.put(queueName, curBlockQueue);
                }

            } catch (InvalidProtocolBufferException e) {
                log.error(e.getMessage(), e);
            }
        }

    }

    private void recoverNormalQueue(String queueName, QueueInfo queueInfo) throws InvalidProtocolBufferException {
        BlockingDeque<MQMessage.MQEntity> curBlockQueue = new LinkedBlockingDeque<>();
        if (StringUtils.isBlank(queueInfo.getOffset())) {
            return;
        }
        List<String> queueJsonData = KrestFileHandler.readData(
                CacheFileConfig.queueCacheDatePath + queueName, queueInfo.getOffset());

        // 开始清洗数据
        for (String jsonData : queueJsonData) {
            MQMessage.MQEntity.Builder tempBuilder = MQMessage.MQEntity.newBuilder();
            JsonFormat.parser().merge((String) JSONObject.parse(jsonData),
                    tempBuilder);
            MQMessage.MQEntity mqEntity = tempBuilder.build();
            if (Long.valueOf(queueInfo.getOffset())
                    .compareTo(Long.valueOf(mqEntity.getId())) <= 0) {
                curBlockQueue.offer(mqEntity);
            }
        }
        log.info("普通持久化队列 -> " + queueName + " : " + curBlockQueue.size());
        BrokerLocalCache.queueInfoMap.get(queueName).setAmount(curBlockQueue.size());
        BrokerLocalCache.queueMap.put(queueName, curBlockQueue);
    }


    private void recoverDelayQueue(String queueName, QueueInfo queueInfo) throws InvalidProtocolBufferException {
        // 开始构建队列
        DelayQueue<DelayMessage> curBlockQueue = new DelayQueue<>();

        if (StringUtils.isBlank(queueInfo.getOffset())) {
            return;
        }

        List<String> queueJsonData = KrestFileHandler.readData(
                CacheFileConfig.queueCacheDatePath + queueName, queueInfo.getOffset());

        // 开始清洗数据
        for (String jsonData : queueJsonData) {
            MQMessage.MQEntity.Builder tempBuilder = MQMessage.MQEntity.newBuilder();
            JsonFormat.parser().merge((String) JSONObject.parse(jsonData),
                    tempBuilder);
            MQMessage.MQEntity mqEntity = tempBuilder.build();
            if (Long.valueOf(queueInfo.getOffset())
                    .compareTo(Long.valueOf(mqEntity.getId())) <= 0) {
                DelayMessage delayMessage = new DelayMessage(mqEntity.getTimeout(), mqEntity);
                curBlockQueue.offer(delayMessage);
            }
        }
        log.info("延时队列 -> " + queueName + " : " + curBlockQueue.size());
        BrokerLocalCache.queueInfoMap.get(queueName).setAmount(curBlockQueue.size());
        BrokerLocalCache.delayQueueMap.put(queueName, curBlockQueue);
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
