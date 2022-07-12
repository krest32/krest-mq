package com.krest.mq.udp;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientFactory;
import com.krest.mq.core.client.MQUDPClient;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.handler.MQTCPClientHandler;
import com.krest.mq.core.handler.MQUDPClientHandler;
import com.krest.mq.core.entity.MQMessage;

import java.util.UUID;

public class UdpClient {
    public static void main(String[] args) {
        MQMessage.MQEntity request = MQMessage.MQEntity.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .addToQueue("demo")
                .build();

        MQClientFactory clientFactory = new MQClientFactory(ConnType.UDP, "localhost", 9001);
        MQClient client = clientFactory.getClient();
        client.connectAndSend(new MQUDPClientHandler(), request);
    }
}
