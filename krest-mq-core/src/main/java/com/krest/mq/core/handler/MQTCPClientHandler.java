package com.krest.mq.core.handler;

import com.krest.mq.core.listener.ChannelInactiveListener;
import com.krest.mq.core.entity.MQMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTCPClientHandler extends SimpleChannelInboundHandler<MQMessage.MQEntity> {

    private ChannelInactiveListener inactiveListener = null;

    public MQTCPClientHandler() {
    }

    public MQTCPClientHandler(ChannelInactiveListener channelInactiveListener) {
        this.inactiveListener = channelInactiveListener;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MQMessage.MQEntity msg) throws Exception {
        System.out.println("收到消息");
        System.out.println(msg.toByteArray().length);
        System.out.println(msg);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        Channel channel = ctx.channel();
        if (channel.isActive()) ctx.close();
    }

    /**
     * 当服务器下线后，自动开始重新链接
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (inactiveListener != null) {
            inactiveListener.onInactive();
        }
    }
}