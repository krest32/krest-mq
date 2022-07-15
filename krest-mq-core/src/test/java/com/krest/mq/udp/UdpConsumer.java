package com.krest.mq.udp;


public class UdpConsumer {
    public static void main(String[] args) {

//        MQBuilderConfig config = MQBuilderConfig.builder().port(9002).remoteAddress("localhost")
//                .remotePort(9001).connType(ConnType.UDP).build();

//        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
//        MQMessage.MQEntity request = builder.setId(UUID.randomUUID().toString())
//                .setIsAck(true)
//                .setMsgType(2)
//                .setMsg("客户端链接成功")
//                .putQueueInfo("demo", 1)
//                .setConnType(2)
//                .setAddress("localhost")
//                .setPort(config.getPort())
//                .build();
//
//        MQClientFactory clientFactory = new MQClientFactory(config);
//        MQClient client = clientFactory.getClient();
//        client.connectAndSend(new MQUDPClientHandler(), request);
    }
}
