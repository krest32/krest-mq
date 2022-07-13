package com.krest.mq.core.processor;

import com.google.protobuf.ProtocolStringList;
import com.krest.file.handler.FileHandler;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.utils.MsgResolver;
import com.krest.mq.core.runnable.ExecutorFactory;
import com.krest.mq.core.runnable.TcpSendMsgRunnable;
import com.krest.mq.core.runnable.ThreadPoolConfig;
import com.krest.mq.core.utils.DateUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * 消息处理中心
 */
@Slf4j
public class TcpMsgProcessor {

    static ThreadPoolExecutor sendMsgExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

    static MQMessage.MQEntity.Builder entityBuilder = MQMessage.MQEntity.newBuilder();

    /**
     * 消息处理逻辑
     */
    public static void msgCenter(ChannelHandlerContext ctx, MQMessage.MQEntity entity) {
        System.out.println("服务端获取消息：");
        System.out.println(entity);
        // 先排除异常情况
        // 1. 没有设定消息的来源
        if (entity.getMsgType() == 0) {
            handlerErr(ctx, entity, "unknown msg type");
            return;
        }

        // 2. 生产着设置的消息队列不存在
        if (entity.getMsgType() == 1) {
            ProtocolStringList toQueueList = entity.getQueueList();
            for (String curQueueName : toQueueList) {
                if (LocalCache.queueMap.get(curQueueName) == null) {
                    handlerErr(ctx, entity, "msg queue does not exist!");
                    return;
                }
            }
        }

        // 开始根据消息类型处理消息
        // 1 代表生产则
        // 2 代表消费者
        int msgType = entity.getMsgType();
        switch (msgType) {
            case 1:
                producer(ctx, entity);
                break;
            case 2:
                consumer(ctx, entity);
                break;
            default:
                handlerErr(ctx, entity, "unknown msg type");
                break;
        }
    }


    /**
     * 返回错误信息
     */
    private static void handlerErr(ChannelHandlerContext ctx, MQMessage.MQEntity entity, String errorMsg) {
        MQMessage.MQEntity response = entityBuilder.setId(entity.getId())
                .setErrFlag(true)
                .setMsg(errorMsg)
                .setDateTime(DateUtils.getNowDate())
                .build();
        ctx.writeAndFlush(response);
    }


    /**
     * 消息同步
     * 1. 消息同步本地
     */
    public static boolean synchMsg(MQMessage.MQEntity entity) {
        return FileHandler.saveData(entity.getId(), entity.toByteArray());
    }


    public static boolean synchMsg(DatagramPacket datagramPacket) {
        return synchMsg(MsgResolver.parseUdpDatagramPacket(datagramPacket));
    }


    public static void msgCenter(ChannelHandlerContext ctx, DatagramPacket datagramPacket) {
        MQMessage.MQEntity entity = MsgResolver.parseUdpDatagramPacket(datagramPacket);
        if (null == LocalCache.udpChannel) {
            LocalCache.udpChannel = ctx.channel();
        }
        msgCenter(ctx, entity);
    }


    /**
     * 1. 找到对象的 msg queue 放入消息，如果没有就创建新的 msg queue
     * 2. 给生产者返回响应的确认标记
     * 3. 获取对应消费者的ctx，然后推送消息，但是需要判断ctx是否存活状态
     */
    private static void producer(ChannelHandlerContext ctx, MQMessage.MQEntity mqEntity) {
        // 整理消息一次放入到每个消息队列中
        ProtocolStringList queueNames = mqEntity.getQueueList();

        if (!queueNames.isEmpty()) {
            // 将消息放入到队列当中，已经对于 队列不存在的情况作处理，此处不作任何处理
            for (String queueName : queueNames) {
                BlockingQueue<MQMessage.MQEntity> queue = LocalCache.queueMap.get(queueName);
                queue.offer(mqEntity);
            }
        }


        // 回复生产者
        if (mqEntity.getIsAck()) {
            MQMessage.MQEntity response = entityBuilder.setId(mqEntity.getId())
                    .setAck(true)
                    .setDateTime(DateUtils.getNowDate())
                    .build();

            System.out.println("返回确认消息");
            System.out.println(response);

            ctx.writeAndFlush(response);
        }
    }


    /**
     * 1。consumer 是否有对应的 msg queue，如果没有就新建
     * 2. server 返回建立链接的信息
     * 3. 如果是推送机制，那么我们就需要主动推送信息
     */
    private static void consumer(ChannelHandlerContext ctx, MQMessage.MQEntity request) {
        // 返回确认信息
        if (request.getIsAck()) {
            MQMessage.MQEntity response = entityBuilder.setId(request.getId())
                    .setAck(true)
                    .setDateTime(DateUtils.getNowDate())
                    .build();
            ctx.writeAndFlush(response);
        }


        Map<String, Integer> queueInfoMap = request.getQueueInfoMap();
        Iterator<Map.Entry<String, Integer>> iterator = queueInfoMap.entrySet().iterator();
        List<String> queueNameList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> queueInfo = iterator.next();
            String queueName = queueInfo.getKey();
            queueNameList.add(queueName);
            int val = queueInfo.getValue();

            // 增加缓存设置
            List<Channel> channels = LocalCache.ctxMap.getOrDefault(queueName, new ArrayList<>());
            channels.add(ctx.channel());
            LocalCache.ctxMap.put(queueName, channels);

            // 新建 queue 同时启动线程池发送信息
            if (LocalCache.queueMap.get(queueName) == null) {
                LocalCache.queueInfoMap.put(queueName, new QueueInfo(queueName,
                        request.getMsgType() == 1 ? QueueType.PERMANENT : QueueType.TEMPORARY));
                // 如果不存在队列 就进行创建queue
                LocalCache.queueMap.put(queueName, new LinkedBlockingQueue<>());
                // 开启推送模式
                sendMsgExecutor.execute(new TcpSendMsgRunnable(queueName));
            }
        }
        LocalCache.ctxQueueMap.put(ctx.channel(), queueNameList);
    }
}

