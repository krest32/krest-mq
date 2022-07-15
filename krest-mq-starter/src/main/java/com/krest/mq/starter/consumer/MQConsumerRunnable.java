package com.krest.mq.starter.consumer;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class MQConsumerRunnable implements Runnable {

    IdWorker idWorker;
    String host;
    int port;
    Object bean;
    Map<String, Integer> queueInfo;

    public MQConsumerRunnable(String host, int port, Map<String, Integer> queueInfo, Object bean, IdWorker idWorker) {
        this.host = host;
        this.port = port;
        this.bean = bean;
        this.idWorker = idWorker;
        this.queueInfo = queueInfo;
    }

    /**
     * 开始建立客户端
     */
    @Override
    public void run() {
        log.info("consumer connect server, host : {} , port : {} ", host, port);

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder
                .setId(String.valueOf(idWorker.nextId()))
                .setDateTime(DateUtils.getNowDate())
                .setMsgType(2)
                .setIsAck(true)
                .putAllQueueInfo(this.queueInfo)
                .build();
        MQTCPClient mqConsumer = new MQTCPClient(host, port, request);
        ChannelListener inactiveListener = mqConsumer.getInactiveListener();
        mqConsumer.connect(new ConsumerChannelInitializer(inactiveListener, bean, request));
        mqConsumer.sendMsg(request);
    }
}
