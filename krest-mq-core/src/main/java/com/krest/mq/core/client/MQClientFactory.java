package com.krest.mq.core.client;

import com.krest.mq.core.config.MQBuilderConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.entity.MQMessage;


public class MQClientFactory {

    MQClient mqClient;
    MQBuilderConfig mqConfig;

    public MQClientFactory(MQBuilderConfig mqConfig) {
        this.mqConfig = mqConfig;
        MQMessage.MQEntity mqEntity = MQMessage.MQEntity.newBuilder().build();
        if (this.mqConfig.getConnType().equals(ConnType.TCP)) {
            mqClient = new MQTCPClient(this.mqConfig.getRemoteAddress(), this.mqConfig.getPort(), mqEntity);
        } else {
            mqClient = new MQUDPClient(this.mqConfig, mqEntity);
        }
    }

    public MQClient getClient() {
        return mqClient;
    }
}
