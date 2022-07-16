package com.krest.mq.starter;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.common.KrestMQTemplate;
import com.krest.mq.starter.producer.RegisterProducer;
import com.krest.mq.starter.properties.KrestMQProperties;
import io.netty.channel.Channel;

public class KrestMQService {

    private KrestMQProperties config;
    private Channel channel;
    private RegisterProducer registerProducer;
    private MQTCPClient mqtcpClient;
    private IdWorker idWorker;

    public KrestMQService(KrestMQProperties config) {
        this.config = config;
        this.idWorker = new IdWorker(
                config.getWorkerId(), config.getDatacenterId(), 1);
        this.registerProducer = new RegisterProducer(this.config,this.idWorker);
        this.mqtcpClient = this.registerProducer.getMqProducer();
        this.channel = this.mqtcpClient.channel;

    }

    public MQTCPClient getMqProducer() {
        return this.mqtcpClient;
    }

    public IdWorker getIdWorker() {
        return this.idWorker;
    }

    public KrestMQTemplate getMQTemplate() {
        return new KrestMQTemplate(this.channel, this.idWorker);
    }
}
