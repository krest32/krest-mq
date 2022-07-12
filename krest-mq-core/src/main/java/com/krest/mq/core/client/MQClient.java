package com.krest.mq.core.client;

import com.krest.mq.core.entity.MQMessage;
import io.netty.channel.ChannelInboundHandlerAdapter;

public interface MQClient {

    void connect(ChannelInboundHandlerAdapter handlerAdapter);

    void connectAndSend(ChannelInboundHandlerAdapter handlerAdapter, MQMessage.MQEntity mqEntity);
}
