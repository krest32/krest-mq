package com.krest.mq.starter.common;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.TransferType;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.core.utils.TcpMsgSendUtils;
import com.krest.mq.starter.producer.MQProducerRunnable;
import com.krest.mq.starter.properties.KrestMQProperties;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;

@Slf4j
public class KrestMQTemplate {

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

    /**
     * 普通的发送消息，默认参数：广播模式、非Ack, 可能会丢失消息
     * 1. 消息内容
     * 2. 队列名称
     */
    public void sendMsg(String msg, String queueName) {
        buildMQReturn(msg, queueName, TransferType.BROADCAST, false, 0L);
    }

    /**
     * 普通的 ack 发送消息，默认参数：广播模式, 会一直等到确认 Ack
     * 1. 消息内容
     * 2. 队列名称
     */
    public boolean sendMsg(String msg, String queueName, Boolean isAck) {
        // 尝试发送消息
        return buildMQReturn(msg, queueName, TransferType.BROADCAST, isAck, 0L);
    }


    public boolean sendMsg(String msg, String queueName, TransferType transferType, Boolean isAck, Long timeout) {
        return buildMQReturn(msg, queueName, transferType, isAck, timeout);
    }

    /**
     * 构建消息，并进行发送
     */
    private boolean buildMQReturn(String msg, String queueName, TransferType transferType, Boolean isAck, Long timeout) {
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder()
                .setId(String.valueOf(this.idWorker.nextId()))
                .setMsgType(1) // 消息生产者
                .addQueue(queueName) // 目标队列
                .setMsg(msg) // 消息内容
                .setIsAck(isAck) // 是否 ack 模式
                .setTransferType(transferType.equals(TransferType.POINT) ? 1 : 2) // 消息接收类型 1. 为单点 2. 为广播
                .setTimeout(timeout)    // ack 模式的超时时间
                .setDateTime(DateUtils.getNowDate())
                .build();
        if (isAck) {
            return TcpMsgSendUtils.ackSendMode(this.tcpClient.channel, mqEntity);
        } else {
            return TcpMsgSendUtils.normalSendMode(this.tcpClient.channel, mqEntity);
        }
    }


}
