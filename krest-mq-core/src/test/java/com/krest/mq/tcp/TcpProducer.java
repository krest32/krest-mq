package com.krest.mq.tcp;

public class TcpProducer {
    public static void main(String[] args) {

//
//        // 构建配置
//        MQBuilderConfig config = MQBuilderConfig.builder().pushMode(true).remoteAddress("localhost").port(9001)
//                .connType(ConnType.TCP)
//                .build();
//
//        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
//        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
//                .setIsAck(true)
//                .setMsgType(1)
//                .setMsg("测试发送消息")
//                .addQueue("demo")
//                .build();
//
//        MQClientFactory clientFactory = new MQClientFactory(config);
//        MQClient client = clientFactory.getClient();
//        client.connectAndSend(new MQTCPClientHandler(), request);
//
//        for (int i = 0; i < 100; i++) {
//
//            MQMessage.MQEntity msg = MQMessage.MQEntity.newBuilder()
//                    .setId(String.valueOf(i))
//                    .setMsg(String.valueOf(i))
//                    .addQueue("demo")
//                    .setIsAck(false)
//                    .setMsgType(1)
//                    .build();
//            System.out.println(msg);
//            client.sendMsg(msg);
//        }
    }
}