package com.krest.mq.core.client;

import com.krest.mq.core.entity.ChannelInactiveListener;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQClientHandler extends ChannelInboundHandlerAdapter {

    private ChannelInactiveListener inactiveListener = null;

    public MQClientHandler() {
    }

    public MQClientHandler(ChannelInactiveListener channelInactiveListener) {
        this.inactiveListener = channelInactiveListener;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("get msg : " + msg);
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
