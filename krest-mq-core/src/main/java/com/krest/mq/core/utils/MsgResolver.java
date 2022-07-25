package com.krest.mq.core.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.runnable.SynchLocalDataRunnable;
import com.krest.mq.core.runnable.MsgDelaySendRunnable;
import com.krest.mq.core.runnable.MsgPutRunnable;
import com.krest.mq.core.runnable.MsgSendRunnable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
public class MsgResolver {

    /**
     * 解析 packet 中的信息
     */
    public static MQMessage.MQEntity parseUdpDatagramPacket(DatagramPacket packet) {
        ByteBuf buf = packet.copy().content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        MQMessage.MQEntity mqEntity = null;
        try {
            mqEntity = MQMessage.MQEntity.parseFrom(req);
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
        }
        return mqEntity;
    }

    /**
     * 构建返回对象信息
     */
    public static DatagramPacket buildUdpDatagramPacket(MQMessage.MQEntity mqEntity, InetSocketAddress socket) {
        DatagramPacket responseData = new DatagramPacket(
                Unpooled.copiedBuffer(mqEntity.toByteArray()), socket);

        return responseData;
    }


    public static void handlerProducerMsg(MQMessage.MQEntity mqEntity) {
        // 整理消息一次放入到每个消息队列中
        ProtocolStringList queueNames = mqEntity.getQueueList();
        if (!queueNames.isEmpty()) {
            for (String queueName : queueNames) {
                // 判断内存中是否存在该队列
                if (null == BrokerLocalCache.queueInfoMap.get(queueName)) {
                    log.info("{}, msg queue does not exist!", queueName);
                } else {
                    if (MQNormalConfig.defaultAckQueue.equals(queueName)) {
                        continue;
                    } else {
                        LocalExecutor.TcpExecutor.execute(new MsgPutRunnable(queueName, mqEntity));
                    }
                }
            }
        }
    }


    public static void handleConsumerMsg(ChannelHandlerContext ctx, MQMessage.MQEntity request) {
        Map<String, Integer> queueInfoMap = request.getQueueInfoMap();
        Iterator<Map.Entry<String, Integer>> iterator = queueInfoMap.entrySet().iterator();
        List<String> queueNameList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> queueInfo = iterator.next();
            String queueName = queueInfo.getKey();
            queueNameList.add(queueName);
            int val = queueInfo.getValue();
            // 增加缓存设置
            List<Channel> channels = BrokerLocalCache.queueCtxListMap.getOrDefault(queueName, new ArrayList<>());
            channels.add(ctx.channel());
            BrokerLocalCache.queueCtxListMap.put(queueName, channels);
            // 开始创建消息队列
            BrokerLocalCache.queueInfoMap.put(queueName, getQueueInfo(queueName, val, request.getId()));
            // 如果不存在队列 就进行创建queue, 并开启监听
            if (BrokerLocalCache.queueInfoMap.get(queueName).getType().equals(QueueType.DELAY)) {

                if (BrokerLocalCache.queueMap.get(queueName) != null) {
                    log.error(queueName + ": 定义为延时队列，但是存在普通队列的 ");
                }
                if (BrokerLocalCache.delayQueueMap.get(queueName) == null) {
                    // 新建延时队列
                    log.info("new delay queue : {}", queueName);
                    BrokerLocalCache.delayQueueMap.put(queueName, new DelayQueue<>());
                }
                LocalExecutor.TcpDelayExecutor.execute(new MsgDelaySendRunnable(queueName));
            } else {

                if (BrokerLocalCache.queueMap.get(queueName) == null) {
                    // 新建普通队列
                    log.info("new normal queue : {}", queueName);
                    BlockingDeque<MQMessage.MQEntity> blockingDeque = new LinkedBlockingDeque<>();
                    BrokerLocalCache.queueMap.put(queueName, blockingDeque);
                }
                LocalExecutor.TcpExecutor.execute(new MsgSendRunnable(queueName));
            }
        }

        // 异步 开启同步任务
        BrokerLocalCache.ctxQueueListMap.put(ctx.channel(), queueNameList);
        LocalExecutor.TcpExecutor.execute(new SynchLocalDataRunnable());
    }


    private static QueueInfo getQueueInfo(String queueName, int val, String offset) {
        QueueInfo queueInfo = new QueueInfo();
        queueInfo.setName(queueName);
        queueInfo.setOffset(offset);
        switch (val) {
            case 1:
                queueInfo.setType(QueueType.PERMANENT);
                break;
            case 2:
                queueInfo.setType(QueueType.TEMPORARY);
                break;
            case 3:
                queueInfo.setType(QueueType.DELAY);
                break;
            default:
                log.error("unknown queue type:{}", queueName);
                queueInfo.setType(QueueType.TEMPORARY);
                break;
        }
        return queueInfo;
    }

}
