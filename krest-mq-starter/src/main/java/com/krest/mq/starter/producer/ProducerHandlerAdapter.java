package com.krest.mq.starter.producer;

import com.krest.mq.core.listener.ChannelInactiveListener;
import com.krest.mq.core.entity.MQMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProducerHandlerAdapter extends SimpleChannelInboundHandler<MQMessage.MQEntity> {

    ChannelInactiveListener inactiveListener;

    private ProducerHandlerAdapter() {
    }

    public ProducerHandlerAdapter(ChannelInactiveListener inactiveListener) {
        this.inactiveListener = inactiveListener;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MQMessage.MQEntity response) throws Exception {
        // todo 分发消息
        System.out.println("生产者获取信息：" + response);
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (inactiveListener != null) {
            inactiveListener.onInactive();
        }
    }
}
