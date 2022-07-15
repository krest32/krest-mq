package com.krest.mq.tcp;


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
