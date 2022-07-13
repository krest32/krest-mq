package com.krest.mq.udp;

import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class UdpServer {
    public static void main(String[] args) {
        MQConfig config = MQConfig.builder()
                .pushMode(true)
                .port(9001)
                .connType(ConnType.UDP)
                .build();

        MQServerFactory serverFactory = new MQServerFactory(config);
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start();
    }
}
