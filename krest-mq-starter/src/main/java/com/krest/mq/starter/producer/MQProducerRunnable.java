package com.krest.mq.starter.producer;


import com.krest.mq.core.entity.MQMessage;

import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.client.MQTCPClient;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

@Slf4j
public class MQProducerRunnable implements Callable {

    IdWorker idWorker;
    String host;
    int port;
    MQTCPClient mqtcpClient;
    MQMessage.MQEntity mqEntity;


    public MQProducerRunnable(String host, int port, IdWorker idWorker, MQTCPClient mqtcpClient, MQMessage.MQEntity mqEntity) {
        this.host = host;
        this.port = port;
        this.idWorker = idWorker;
        this.mqtcpClient = mqtcpClient;
        this.mqEntity = mqEntity;
    }


    @Override
    public Object call() throws Exception {
        this.mqtcpClient = new MQTCPClient(this.mqEntity);
        this.mqtcpClient.connect(new ProducerChannelInitializer(
                this.mqtcpClient.getInactiveListener(), this.mqEntity));
        log.info("producer connect server, host : {} , port : {} ", this.host, this.port);
        return this.mqtcpClient;
    }
}
