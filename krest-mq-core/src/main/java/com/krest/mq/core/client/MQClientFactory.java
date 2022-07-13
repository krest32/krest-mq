package com.krest.mq.core.client;

import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.ConnType;



public class MQClientFactory {

    MQClient mqClient;
    MQConfig mqConfig;

    public MQClientFactory(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
        if (this.mqConfig.getConnType().equals(ConnType.TCP)) {
            mqClient = new MQTCPClient(this.mqConfig.getRemoteAddress(), this.mqConfig.getPort());
        } else {
            mqClient = new MQUDPClient(this.mqConfig);
        }
    }

    public MQClient getClient() {
        return mqClient;
    }
}
