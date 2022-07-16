package com.krest.mq.starter.common;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.core.utils.MsgSendUtils;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutionException;

public class KrestMQTemplate {

    Channel channel;

    IdWorker idWorker;

    public KrestMQTemplate(Channel channel, IdWorker idWorker) {
        this.channel = channel;
        this.idWorker = idWorker;
    }

    public void sendMsg(String msg, String queueName) throws Throwable {
        sendMsg(msg, queueName, false);
    }

    public boolean sendMsg(String msg, String queueName, Boolean isAck) throws ExecutionException, InterruptedException {
        // 尝试发送消息
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                .setId(String.valueOf(this.idWorker.nextId()))
                .setMsgType(1)
                .addQueue(queueName)
                .setMsg(msg)
                .setIsAck(isAck)
                .setDateTime(DateUtils.getNowDate())
                .build();
        if (isAck) {
            return MsgSendUtils.ackSendMode(this.channel, mqEntity);
        } else {
            return MsgSendUtils.normalSendMode(this.channel, mqEntity);
        }
    }
}
