package com.krest.mq.starter.producer;

import com.krest.mq.core.entity.ChannelInactiveListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@ChannelHandler.Sharable
public class ProducerHandlerAdapter extends ChannelInboundHandlerAdapter {

    ChannelInactiveListener inactiveListener;

    private ProducerHandlerAdapter() {
    }

    public ProducerHandlerAdapter(ChannelInactiveListener inactiveListener) {
        this.inactiveListener = inactiveListener;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // todo 分发消息
        System.out.println("生产者获取信息：" + msg);
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
