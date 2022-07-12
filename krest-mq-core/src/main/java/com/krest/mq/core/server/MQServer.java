package com.krest.mq.core.server;

import io.netty.channel.ChannelInboundHandlerAdapter;

public interface MQServer {
    void start(ChannelInboundHandlerAdapter handlerAdapter);
}
