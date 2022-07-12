package com.krest.mq.tcp;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientFactory;
import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.handler.MQTCPClientHandler;
import com.krest.mq.core.entity.MQMessage;

import java.util.UUID;

public class TcpClient {
    public static void main(String[] args) {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .addToQueue("demo")
                .build();

        MQClientFactory clientFactory = new MQClientFactory(ConnType.TCP, "localhost", 9001);
        MQClient client = clientFactory.getClient();
        client.connectAndSend(new MQTCPClientHandler(), request);
    }
}
