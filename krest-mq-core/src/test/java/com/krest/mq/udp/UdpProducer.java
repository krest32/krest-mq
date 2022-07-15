package com.krest.mq.udp;

public class UdpProducer {
    public static void main(String[] args) {
//
//        // 构建配置
//        MQBuilderConfig config = MQBuilderConfig.builder().remoteAddress("localhost").remotePort(9001)
//                .port(9003).connType(ConnType.UDP).build();
//
//        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
//        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
//                .setIsAck(true).setConnType(2).setMsgType(1).setMsg("连接")
//                .addQueue("demo").setPort(config.getPort()).build();
//
//        MQClientFactory clientFactory = new MQClientFactory(config);
//        MQClient client = clientFactory.getClient();
//        client.connectAndSend(new MQUDPClientHandler(), request);
////
//        for (int i = 0; i < 10; i++) {
//            MQMessage.MQEntity msg = MQMessage.MQEntity.newBuilder()
//                    .setId(String.valueOf(i))
//                    .setMsg(String.valueOf(i))
//                    .addQueue("demo")
//                    .setConnType(2)
//                    .setIsAck(false)
//                    .setMsgType(1)
//                    .build();
//            System.out.println(msg);
//            client.sendMsg(msg);
//        }
    }
}
