package com.krest.mq.starter.producer;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.config.MQNormalConfig;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import com.krest.mq.starter.common.KrestMQTemplate;
import com.krest.mq.starter.consumer.ConsumerChannelInitializer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.Callable;

@Slf4j
public class MQProducerRunnable implements Callable {

    IdWorker idWorker;
    String host;
    int port;
    MQTCPClient mqtcpClient;


    public MQProducerRunnable(String host, int port, IdWorker idWorker, MQTCPClient mqtcpClient) {
        this.host = host;
        this.port = port;
        this.idWorker = idWorker;
        this.mqtcpClient = mqtcpClient;
    }


    /**
     * 设置注册的 Msg
     */
    private MQMessage.MQEntity registerMsg() {
        MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
        return builder.setId(String.valueOf(this.idWorker.nextId()))
                .setIsAck(true)
                .setMsgType(1)
                .setDateTime(DateUtils.getNowDate())
                .addQueue(MQNormalConfig.defaultAckQueue)
                .build();
    }

    @Override
    public Object call() throws Exception {
        log.info("consumer connect server, host : {} , port : {} ", host, port);
        MQMessage.MQEntity registerMsg = registerMsg();
        this.mqtcpClient = new MQTCPClient(this.host, this.port, registerMsg);
        this.mqtcpClient.connect(new ProducerChannelInitializer(
                this.mqtcpClient.getInactiveListener(), registerMsg));
        return this.mqtcpClient;
    }
}
