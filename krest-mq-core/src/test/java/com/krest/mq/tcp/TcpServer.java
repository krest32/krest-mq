package com.krest.mq.tcp;

import com.krest.mq.core.config.MQBuilderConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class TcpServer {
    public static void main(String[] args) {
        MQServerFactory serverFactory = new MQServerFactory();
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start();
    }
}
