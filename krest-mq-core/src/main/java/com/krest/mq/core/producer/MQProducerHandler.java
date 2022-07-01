package com.krest.mq.core.producer;

import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.utils.MQUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MQProducerHandler extends ChannelInboundHandlerAdapter {

    ChannelInactiveListener inactiveListener = null;


    public MQProducerHandler(ChannelInactiveListener inactiveListener) {
        this.inactiveListener = inactiveListener;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("生产者得到消息: {}", msg);
        // 关闭连接
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        Channel channel = ctx.channel();
        if (channel.isActive()) ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (inactiveListener != null) {
            inactiveListener.onInactive();
        }
    }
}
