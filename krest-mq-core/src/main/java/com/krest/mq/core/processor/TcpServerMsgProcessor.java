package com.krest.mq.core.processor;

import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.entity.QueueType;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.runnable.*;
import com.krest.mq.core.utils.DateUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 消息处理中心
 */
@Slf4j
public class TcpServerMsgProcessor {


    /**
     * 消息处理逻辑
     */
    public static void msgCenter(ChannelHandlerContext ctx, MQMessage.MQEntity entity) {
        System.out.println("服务端获取消息：");
        System.out.println("----------------------");
        System.out.println(entity);
        System.out.println("----------------------");


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
            case 3:
                responseMsg(entity);
                break;
            default:
                handlerErr(ctx, entity, "unknown msg type");
                break;
        }
    }

    private static void responseMsg(MQMessage.MQEntity entity) {
        // 添加到回复的队列当中
        LocalCache.responseQueue.add(entity);
    }


    /**
     * 返回错误信息
     */
    private static void handlerErr(ChannelHandlerContext ctx, MQMessage.MQEntity entity, String errorMsg) {
        MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder().setId(entity.getId())
                .setErrFlag(true)
                .setMsg(errorMsg)
                .setDateTime(DateUtils.getNowDate())
                .build();
        System.out.println(response);
//        ctx.writeAndFlush(response);
    }


    /**
     * 1. 找到对象的 msg queue 放入消息，如果没有就创建新的 msg queue
     * 2. 给生产者返回响应的确认标记
     * 3. 获取对应消费者的ctx，然后推送消息，但是需要判断ctx是否存活状态
     */
    private static void producer(ChannelHandlerContext ctx, MQMessage.MQEntity mqEntity) {
        // 判断 消息队列 是否存在
        ProtocolStringList toQueueList = mqEntity.getQueueList();
        for (String curQueueName : toQueueList) {
            if (LocalCache.queueInfoMap.get(curQueueName) == null) {
                handlerErr(ctx, mqEntity, "msg queue does not exist!");
                return;
            }
        }


        // 整理消息一次放入到每个消息队列中
        ProtocolStringList queueNames = mqEntity.getQueueList();
        if (!queueNames.isEmpty()) {
            for (String queueName : queueNames) {
                if (MQNormalConfig.defaultAckQueue.equals(queueName)) {
                    continue;
                }
                // 将消息放入到队列当中，已经对于 队列不存在的情况作处理，此处不作任何处理
                LocalExecutor.TcpExecutor.execute(new TcpPutMsgRunnable(queueName, mqEntity));
            }
        }

        // 回复消息： 不需要
        if (mqEntity.getIsAck()) {
            MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder()
                    .setId(mqEntity.getId())
                    .setMsgType(3)
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
            MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder()
                    .setId(request.getId())
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
            List<Channel> channels = LocalCache.queueCtxListMap.getOrDefault(queueName, new ArrayList<>());
            channels.add(ctx.channel());
            LocalCache.queueCtxListMap.put(queueName, channels);

            // 开始创建消息队列
            LocalCache.queueInfoMap.put(queueName, getQueueInfo(queueName, val));
            // 如果不存在队列 就进行创建queue, 并开启监听
            if (LocalCache.queueInfoMap.get(queueName).getType().equals(QueueType.DELAY)) {
                if (LocalCache.queueMap.get(queueName) != null) {
                    log.error(queueName + ": 定义为延时队列，但是存在普通队列的 ");
                }
                if (LocalCache.delayQueueMap.get(queueName) == null) {
                    // 新建延时队列
                    log.info("new delay queue : {}", queueName);
                    LocalCache.delayQueueMap.put(queueName, new DelayQueue<>());
                }
                LocalExecutor.TcpDelayExecutor.execute(new TcpDelayMsgSendRunnable(queueName));
            } else {
                if (LocalCache.queueMap.get(queueName) == null) {
                    log.info("new normal queue : {}", queueName);
                    LocalCache.queueMap.put(queueName, new LinkedBlockingDeque<>());
                }
                LocalExecutor.TcpExecutor.execute(new TcpSendMsgRunnable(queueName));
            }
        }


        // 异步 开启同步任务
        LocalCache.ctxQueueListMap.put(ctx.channel(), queueNameList);
        LocalExecutor.TcpExecutor.execute(new SynchCacheRunnable());
    }

    private static QueueInfo getQueueInfo(String queueName, int val) {
        QueueInfo queueInfo = new QueueInfo();
        queueInfo.setName(queueName);
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

