package com.krest.mq.builder;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientFactory;
import com.krest.mq.core.entity.ConnType;

import com.krest.mq.core.handler.MQTCPServerHandler;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class Dmeo {
    public static void main(String[] args) {
        MQServerFactory serverFactory = new MQServerFactory(ConnType.TCP, 9001);
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start(null);

        MQClientFactory clientFactory = new MQClientFactory(ConnType.TCP, "localhost", 9001);
        MQClient client = clientFactory.getClient();
        client.connect(null);
    }
}
