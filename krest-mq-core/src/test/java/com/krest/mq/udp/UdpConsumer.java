package com.krest.mq.udp;


import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientFactory;
import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.handler.MQUDPClientHandler;

import java.util.UUID;

public class UdpConsumer {
    public static void main(String[] args) {

        MQConfig config = MQConfig.builder().port(9002).remoteAddress("localhost")
                .remotePort(9001).connType(ConnType.UDP).build();

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(2)
                .setMsg("客户端链接成功")
                .putQueueInfo("demo", 1)
                .setConnType(2)
                .setAddress("localhost")
                .setPort(config.getPort())
                .build();

        MQClientFactory clientFactory = new MQClientFactory(config);
        MQClient client = clientFactory.getClient();
        client.connectAndSend(new MQUDPClientHandler(), request);
    }
}
