package com.krest.mq.core.server;

import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.ConnType;
import lombok.Data;

@Data
public class MQServerFactory {
    private MQConfig mqConfig;
    private MQServer mqServer;

    private MQServerFactory() {
    }

    public MQServerFactory(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
        if (mqConfig.getConnType().equals(ConnType.TCP)) {
            mqServer = new MQTCPServer(this.mqConfig);
        } else {
            mqServer = new MQUDPServer(this.mqConfig);
        }
    }

    public MQServer getServer() {
        return this.mqServer;
    }
}
