package com.krest.mq.starter.common;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.core.utils.MsgSendUtils;
import com.krest.mq.starter.producer.MQProducerRunnable;
import com.krest.mq.starter.properties.KrestMQProperties;
import io.netty.channel.Channel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

@Slf4j
public class KrestMQTemplate {

    public Channel channel;
    IdWorker idWorker;
    MQTCPClient tcpClient;

    public KrestMQTemplate(IdWorker idWorker, KrestMQProperties config) {
        this.idWorker = idWorker;
        MQProducerRunnable runnable = new MQProducerRunnable(
                config.getHost(), config.getPort(), this.idWorker, this.tcpClient);
        FutureTask<MQTCPClient> futureTask = new FutureTask(runnable);
        Thread t = new Thread(futureTask);
        t.start();
        try {
            this.tcpClient = futureTask.get();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
        }
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
            return MsgSendUtils.ackSendMode(this.tcpClient.channel, mqEntity);
        } else {
            return MsgSendUtils.normalSendMode(this.tcpClient.channel, mqEntity);
        }
    }
}
