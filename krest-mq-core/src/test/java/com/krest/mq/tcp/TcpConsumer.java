package com.krest.mq.tcp;


import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientFactory;
import com.krest.mq.core.config.MQBuilderConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MQTCPClientHandler;

import java.util.UUID;

public class TcpConsumer {
    public static void main(String[] args) {

        // 构建配置
//        MQBuilderConfig config = MQBuilderConfig.builder().remoteAddress("localhost").port(9001)
//                .connType(ConnType.TCP).build();
//
//        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
//        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
//                .setIsAck(true)
//                .setMsgType(2)
//                .putQueueInfo("demo", 1)
//                .build();
//
//        MQClientFactory clientFactory = new MQClientFactory(config);
//        MQClient client = clientFactory.getClient();
//        client.connectAndSend(new MQTCPClientHandler(), request);
    }
}
