package com.krest.mq.starter.producer;

import com.krest.mq.core.listener.ChannelListener;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.processor.TcpClientMsgProcessor;
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
        TcpClientMsgProcessor.msgCenter(channelHandlerContext, response);
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (inactiveListener != null) {
            inactiveListener.onInactive(this.mqEntity);
        }
    }
}
