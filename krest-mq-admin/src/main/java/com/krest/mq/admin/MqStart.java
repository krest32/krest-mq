package com.krest.mq.admin;

import com.krest.mq.core.server.MqServer;

public class MqStart {
    public static void main(String[] args) {
        MqServer mqServer = new MqServer(9001,true);
        mqServer.start();
    }
}
