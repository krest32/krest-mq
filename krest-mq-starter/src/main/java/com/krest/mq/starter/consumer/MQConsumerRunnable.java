package com.krest.mq.starter.consumer;

import com.krest.mq.core.client.MQTCPClient;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.utils.DateUtils;
import com.krest.mq.core.utils.IdWorker;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class MQConsumerRunnable implements Runnable {


    String host;
    int port;
    Object bean;
    MQMessage.MQEntity requestMSg;

    public MQConsumerRunnable(String host, int port,
                              Object bean,  MQMessage.MQEntity requestMSg) {
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
        log.info("consumer connect server, host : {} , port : {} ", host, port);
        MQTCPClient mqConsumer = new MQTCPClient(host, port, this.requestMSg);
        ChannelListener inactiveListener = mqConsumer.getInactiveListener();
        mqConsumer.connect(new ConsumerChannelInitializer(inactiveListener, bean, this.requestMSg));
        mqConsumer.sendMsg(this.requestMSg);
    }


}
