package com.krest.mq.starter.consumer;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.starter.client.ChannelListener;
import com.krest.mq.starter.client.MQTCPClient;
import com.krest.mq.starter.producer.ProducerChannelInitializer;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MQConsumerRunnable implements Runnable {


    String host;
    int port;
    Object bean;
    MQMessage.MQEntity requestMSg;

    public MQConsumerRunnable(String host, int port,
                              Object bean, MQMessage.MQEntity requestMSg) {
        this.host = host;
        this.port = port;
        this.bean = bean;
        this.requestMSg = requestMSg;
    }

    /**
     * 开始建立客户端
     */
    @Override
    public void run() {
        MQTCPClient mqConsumer = new MQTCPClient(this.requestMSg);
        mqConsumer.connect(new ConsumerChannelInitializer(
                mqConsumer.getInactiveListener(), bean, this.requestMSg));

        mqConsumer.sendMsg(this.requestMSg);


    }
}
