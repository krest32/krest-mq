package com.krest.mq.starter;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;
import com.krest.mq.core.entity.MsgStatus;
import com.krest.mq.core.producer.MQProducerHandler;
import com.krest.mq.starter.producer.ProducerHandlerAdapter;
import com.krest.mq.starter.properties.KrestMQProperties;

public class KrestMQService {

    private KrestMQProperties config;

    public KrestMQService(KrestMQProperties config) {
        this.config = config;
    }

    public MQClient getMqProducer() {

        MQEntity entity = new MQEntity("producer connect info ",
                "default", ModuleType.PRODUCER);
        // 链接到Server
        entity.setMsgStatus(MsgStatus.PRODUCER_CONNECT_SERVER);

        MQClient client = new MQClient(config.getHost(), config.getPort(), entity);
        ChannelInactiveListener inactiveListener = client.getInactiveListener();
        ProducerHandlerAdapter handlerAdapter = new ProducerHandlerAdapter(inactiveListener);
        client.connect(handlerAdapter);
        return client;
    }

}
