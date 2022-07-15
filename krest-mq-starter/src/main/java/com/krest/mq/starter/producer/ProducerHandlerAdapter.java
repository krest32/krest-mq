package com.krest.mq.starter.producer;

import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.entity.MQMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProducerHandlerAdapter extends SimpleChannelInboundHandler<MQMessage.MQEntity> {

    ChannelListener inactiveListener;
    MQMessage.MQEntity mqEntity;


    public ProducerHandlerAdapter(ChannelListener inactiveListener, MQMessage.MQEntity mqEntity) {
        this.mqEntity = mqEntity;
        this.inactiveListener = inactiveListener;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MQMessage.MQEntity response)
            throws Exception {
        System.out.println("生产者获取信息：" + response);
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (inactiveListener != null) {
            inactiveListener.onInactive(this.mqEntity);
        }
    }
}
