package com.krest.mq.file;

import com.krest.mq.core.entity.MQMessage;

import java.util.UUID;

public class DemoFile {
    public static void main(String[] args) {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(2)
                .putQueueInfo("demo", 1)
                .build();
    }
}
