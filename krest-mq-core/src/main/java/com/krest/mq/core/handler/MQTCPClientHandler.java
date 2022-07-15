package com.krest.mq.core.handler;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.listener.ChannelListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTCPClientHandler extends SimpleChannelInboundHandler<MQMessage.MQEntity> {

    private ChannelListener inactiveListener = null;
    MQMessage.MQEntity mqEntity;

    public MQTCPClientHandler() {
    }

    public MQTCPClientHandler(ChannelListener channelListener, MQMessage.MQEntity mqEntity) {
        this.inactiveListener = channelListener;
        this.mqEntity = mqEntity;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MQMessage.MQEntity msg) throws Exception {
        System.out.println("收到消息");
        System.out.println(msg.getMsg());
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("收到异常");
    }

    /**
     * 当服务器下线后，自动开始重新链接
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (inactiveListener != null) {
            inactiveListener.onInactive(this.mqEntity);
        }
    }
}
