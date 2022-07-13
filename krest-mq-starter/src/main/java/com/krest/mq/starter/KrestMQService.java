package com.krest.mq.starter;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.starter.producer.ProducerHandlerAdapter;
import com.krest.mq.starter.properties.KrestMQProperties;

import java.util.UUID;

public class KrestMQService {

    private KrestMQProperties config;

    public KrestMQService(KrestMQProperties config) {
        this.config = config;
    }

    public MQTCPClient getMqProducer() {

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue("default")
                .build();

        MQTCPClient client = new MQTCPClient(config.getHost(), config.getPort());
        ChannelListener inactiveListener = client.getInactiveListener();
        ProducerHandlerAdapter handlerAdapter = new ProducerHandlerAdapter(inactiveListener);
        client.connect(handlerAdapter);
        return client;
    }

}
