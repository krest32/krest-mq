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
    private IdWorker idWorker;

    public RegisterProducer(KrestMQProperties config, IdWorker idWorker) {
        this.config = config;
        this.idWorker = idWorker;
    }

    public MQTCPClient getMqProducer() {
        MQMessage.MQEntity registerMsg = registerMsg();
        MQTCPClient producerClient = new MQTCPClient(config.getHost(), config.getPort(), registerMsg);
        producerClient.connect(new ProducerChannelInitializer(
                producerClient.getInactiveListener(), registerMsg));
        return producerClient;
    }

    /**
     * 设置注册的 Msg
     */
    private MQMessage.MQEntity registerMsg() {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        return builder.setId(String.valueOf(this.idWorker.nextId()))
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue(MQNormalConfig.defaultAckQueue)
                .build();
    }
}
