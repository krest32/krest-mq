package com.krest.mq.starter.consumer;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Slf4j
public class MQConsumerRunnable implements Runnable {

    String host;
    int port;
    Object bean;
    Set<String> queueName;

    public MQConsumerRunnable(String host, int port, Set<String> queueName, Object bean) {
        this.host = host;
        this.port = port;
        this.queueName = queueName;
        this.bean = bean;
    }

    /**
     * 开始建立客户端
     */
    @Override
    public void run() {
        log.info("consumer connect server, host : {} , port : {} , queue name : {} "
                , host, port, queueName);
        List<String> queueList = new ArrayList<>(queueName);
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setDateTime(DateUtils.getNowDate())
                .setMsgType(0)
                .setIsAck(true)
                .addAllToQueue(queueList)
                .build();
        MQClient mqConsumer = new MQClient(host, port, request);

        ChannelInactiveListener inactiveListener = mqConsumer.getInactiveListener();
        ConsumerHandlerAdapter handlerAdapter = new ConsumerHandlerAdapter(inactiveListener, this.bean);
        mqConsumer.connect(handlerAdapter);
    }
}
