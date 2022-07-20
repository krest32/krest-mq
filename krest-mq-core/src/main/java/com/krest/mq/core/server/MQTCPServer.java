package com.krest.mq.core.server;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MqTcpServerHandler;
import com.krest.mq.core.runnable.UdpServerRunnable;
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

        log.info("初始化数据完成");
        // 异步的方式启动 udpServer
//        UdpServerRunnable runnable = new UdpServerRunnable(this.port);
//        FutureTask<MQUDPServer> futureTask = new FutureTask<>(runnable);
//        Thread t = new Thread(futureTask);
//        t.start();
    }


    private void recoverQueueData() {

        BrokerLocalCache.queueMap.put(MQNormalConfig.defaultAckQueue, new LinkedBlockingDeque<>(100));
        Iterator<Map.Entry<String, QueueInfo>> iterator = BrokerLocalCache.queueInfoMap.entrySet().iterator();
        log.info("queue info : " + BrokerLocalCache.queueInfoMap);
        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> entry = iterator.next();
            String queueName = entry.getKey();
            QueueInfo queueInfo = entry.getValue();
            try {
                if (!queueInfo.getType().equals(QueueType.TEMPORARY)) {
                    // 开始构建队列
                    BlockingDeque<MQMessage.MQEntity> curBlockQueue = new LinkedBlockingDeque<>();
                    if (StringUtils.isBlank(queueInfo.getOffset())) {
                        continue;
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
                                .compareTo(Long.valueOf(mqEntity.getId())) < 0) {
                            curBlockQueue.offer(mqEntity);
                        }
                    }

                    log.info(queueName + " : " + curBlockQueue.size());
                    BrokerLocalCache.queueMap.put(queueName, curBlockQueue);
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
