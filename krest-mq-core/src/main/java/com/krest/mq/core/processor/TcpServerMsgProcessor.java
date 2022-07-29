package com.krest.mq.core.processor;

import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.runnable.*;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.MsgResolver;
import com.krest.mq.core.utils.SyncUtil;
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


    public static void msgCenter(ChannelHandlerContext ctx, MQMessage.MQEntity entity) {
        // 开始根据消息类型处理消息 1. 生产者  2. 消费者  3. 回复类型消息
        switch (entity.getMsgType()) {
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
                log.error("unknown msg type");
                break;
        }
    }

    private static void responseMsg(MQMessage.MQEntity entity) {
        // 添加到回复的队列当中
        BrokerLocalCache.responseQueue.add(entity);
    }


    /**
     * 1. 找到对象的 msg queue 放入消息，如果没有就创建新的 msg queue
     * 2. 给生产者返回响应的确认标记
     * 3. 获取对应消费者的ctx，然后推送消息，但是需要判断ctx是否存活状态
     */
    private static void producer(ChannelHandlerContext ctx, MQMessage.MQEntity mqEntity) {

        // 将消息放入到队列中
        MsgResolver.handlerProducerMsg(mqEntity);

        // 将消息同步给其他的队列
        if (null != AdminServerCache.clusterInfo.getCurServers()
                && AdminServerCache.clusterInfo.getCurServers().size() > 1) {
            SyncUtil.msgToOtherServer(mqEntity);
        }

        // 判断是否需要回复消息
        if (mqEntity.getIsAck()) {
            MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder()
                    .setId(mqEntity.getId())
                    .setMsgType(3)
                    .setDateTime(DateUtils.getNowDate())
                    .build();
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

        MsgResolver.handleConsumerMsg(ctx, request);
    }


}

