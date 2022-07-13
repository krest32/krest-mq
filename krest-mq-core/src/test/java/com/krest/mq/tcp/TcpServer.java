package com.krest.mq.tcp;

import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class TcpServer {
    public static void main(String[] args) {
        // 构建配置
        MQConfig config = MQConfig.builder().pushMode(true).port(9001).connType(ConnType.TCP).build();

        MQServerFactory serverFactory = new MQServerFactory(config);
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start();

    }
}
