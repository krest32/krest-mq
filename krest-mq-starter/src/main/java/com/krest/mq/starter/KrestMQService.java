package com.krest.mq.starter;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.starter.producer.ProducerChannelInitializer;
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

        System.out.println(config.getHost());
        MQTCPClient producerClient = new MQTCPClient(config.getHost(), config.getPort());
//        MQTCPClient client = new MQTCPClient("localhost", 9001);
        ChannelListener inactiveListener = producerClient.getInactiveListener();
        ProducerHandlerAdapter handlerAdapter = new ProducerHandlerAdapter(inactiveListener);
//        producerClient.connect(handlerAdapter);
        producerClient.connect(new ProducerChannelInitializer(inactiveListener));
        return producerClient;
    }

}
