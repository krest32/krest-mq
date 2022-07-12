package com.krest.mq.udp;

import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.handler.MQUDPServerHandler;
import com.krest.mq.core.server.MQServer;
import com.krest.mq.core.server.MQServerFactory;

public class UdpServer {
    public static void main(String[] args) {
        MQServerFactory serverFactory = new MQServerFactory(ConnType.UDP, 9001);
        MQServer mqServer = serverFactory.getMqServer();
        mqServer.start(new MQUDPServerHandler(true));
    }
}
