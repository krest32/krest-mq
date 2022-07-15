package com.krest.mq.starter.producer;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.producer.ProducerChannelInitializer;
import com.krest.mq.starter.producer.ProducerHandlerAdapter;
import com.krest.mq.starter.properties.KrestMQProperties;

import java.util.UUID;

public class RegisterProducer {

    private KrestMQProperties config;

    public RegisterProducer(KrestMQProperties config) {
        this.config = config;
    }

    public MQTCPClient getMqProducer() {

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue(MQNormalConfig.defaultAckQueue)
                .build();

        MQTCPClient producerClient = new MQTCPClient(config.getHost(), config.getPort(), request);
        ChannelListener inactiveListener = producerClient.getInactiveListener();
        producerClient.connect(new ProducerChannelInitializer(inactiveListener, request));
        return producerClient;
    }

    public IdWorker getIdWorker() {
        IdWorker idWorker = new IdWorker(config.getWorkerId(), config.getDatacenterId(), 1);
        return idWorker;
    }
}
