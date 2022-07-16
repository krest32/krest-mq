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


@Slf4j
public class TcpClientMsgProcessor {

    /**
     * 消息处理逻辑
     */

    public static void msgCenter(ChannelHandlerContext ctx, MQMessage.MQEntity entity) {
        System.out.println("客戶端获取消息：");
        System.out.println("----------------------");
        System.out.println(entity);
        System.out.println("----------------------");

        // 先排除异常情况
        // 1. 没有设定消息的来源
        if (entity.getMsgType() == 0) {
            handlerErr(ctx, entity, "unknown msg type");
            return;
        }

        // 开始根据消息类型处理消息
        // 1 代表生产则
        // 2 代表消费者
        int msgType = entity.getMsgType();
        switch (msgType) {
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
        ctx.writeAndFlush(response);
    }


}
