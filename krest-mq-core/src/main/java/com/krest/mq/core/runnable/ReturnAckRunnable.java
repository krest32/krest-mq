package com.krest.mq.core.runnable;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import io.netty.channel.ChannelHandlerContext;

public class ReturnAckRunnable implements Runnable {
    MQMessage.MQEntity mqEntity;
    ChannelHandlerContext ctx;

    public ReturnAckRunnable(MQMessage.MQEntity mqEntity, ChannelHandlerContext ctx) {
        this.mqEntity = mqEntity;
        this.ctx = ctx;
    }

    @Override
    public void run() {
        MQMessage.MQEntity response = MQMessage.MQEntity.newBuilder()
                .setId(this.mqEntity.getId())
                .setMsgType(3)
                .setDateTime(DateUtils.getNowDate())
                .build();
        this.ctx.writeAndFlush(response);
    }
}
