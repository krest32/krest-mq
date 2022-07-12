package com.krest.mq;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientHandler;
import com.krest.mq.core.entity.MQMessage;

import java.util.UUID;

public class MqClient {
    public static void main(String[] args) {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .addToQueue("demo")
                .build();
        MQClient client = new MQClient("localhost",9001,request);
        client.connect(new MQClientHandler());
    }
}
