package com.krest.mq.starter.consumer;

import com.krest.mq.core.client.MQClient;
import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;
import com.krest.mq.core.entity.MsgStatus;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
public class MQConsumerRunnable implements Runnable {

    String host;
    int port;
    Object bean;
    Set<String> queueName;

    public MQConsumerRunnable(String host, int port, Set<String> queueName, Object bean) {
        this.host = host;
        this.port = port;
        this.queueName = queueName;
        this.bean = bean;
    }

    /**
     * 开始建立客户端
     */
    @Override
    public void run() {
        log.info("consumer connect server, host : {} , port : {} , queue name : {} "
                , host, port, queueName);

        MQEntity entity = new MQEntity("consumer connect server", queueName, ModuleType.CONSUMER);
        entity.setMsgStatus(MsgStatus.CONSUMER_CONNECT_SERVER);
        MQClient mqConsumer = new MQClient(host, port, entity);

        ChannelInactiveListener inactiveListener = mqConsumer.getInactiveListener();
        ConsumerHandlerAdapter handlerAdapter = new ConsumerHandlerAdapter(inactiveListener, this.bean);
        mqConsumer.connect(handlerAdapter);
    }
}
