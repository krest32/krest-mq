package com.krest.mq.starter;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.entity.*;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.starter.producer.ProducerHandlerAdapter;
import com.krest.mq.starter.properties.KrestMQProperties;

import java.util.UUID;

public class KrestMQService {

    private KrestMQProperties config;

    public KrestMQService(KrestMQProperties config) {
        this.config = config;
    }

    public MQClient getMqProducer() {

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addToQueue("default")
                .build();

        MQClient client = new MQClient(config.getHost(), config.getPort(), request);
        ChannelInactiveListener inactiveListener = client.getInactiveListener();
        ProducerHandlerAdapter handlerAdapter = new ProducerHandlerAdapter(inactiveListener);
        client.connect(handlerAdapter);
        return client;
    }

}
