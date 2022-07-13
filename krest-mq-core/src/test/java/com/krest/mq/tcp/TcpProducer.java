package com.krest.mq.tcp;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.client.MQClientFactory;
import com.krest.mq.core.config.MQConfig;
import com.krest.mq.core.entity.ConnType;
import com.krest.mq.core.handler.MQTCPClientHandler;
import com.krest.mq.core.entity.MQMessage;

import java.util.UUID;

public class TcpProducer {
    public static void main(String[] args) {


        // 构建配置
        MQConfig config = MQConfig.builder().pushMode(true).remoteAddress("localhost").port(9001)
                .connType(ConnType.TCP)
                .build();

        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
                .setIsAck(true)
                .setMsgType(1)
                .setMsg("测试发送消息")
                .addQueue("demo")
                .build();

        MQClientFactory clientFactory = new MQClientFactory(config);
        MQClient client = clientFactory.getClient();
        client.connectAndSend(new MQTCPClientHandler(), request);

        for (int i = 0; i < 100; i++) {

            MQMessage.MQEntity msg = MQMessage.MQEntity.newBuilder()
                    .setId(String.valueOf(i))
                    .setMsg(String.valueOf(i))
                    .addQueue("demo")
                    .setIsAck(false)
                    .setMsgType(1)
                    .build();
            System.out.println(msg);
            client.sendMsg(msg);
        }
    }
}
