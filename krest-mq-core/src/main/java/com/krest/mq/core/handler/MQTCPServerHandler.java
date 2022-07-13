package com.krest.mq.core.handler;

import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.processor.TcpMsgProcessor;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
public class MQTCPServerHandler extends SimpleChannelInboundHandler<MQMessage.MQEntity> {


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MQMessage.MQEntity request) {
        TcpMsgProcessor.msgCenter(ctx, request);
    }


    @Override
    public synchronized void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Channel channel = ctx.channel();
        LocalCache.clientChannels.remove(channel);
        // 更新本地缓存的 channel 信息
        List<String> queueList = LocalCache.ctxQueueMap.get(channel);

        if (null != queueList) {
            for (String queueName : queueList) {
                List<Channel> channels = LocalCache.ctxMap.get(queueName);
                channels.remove(channel);
                System.out.println(channels.size());
                LocalCache.ctxMap.put(queueName, channels);
            }
        }

        LocalCache.ctxQueueMap.remove(channel);
        log.info("exceptionCaught client {} 下线了", channel.remoteAddress());
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        LocalCache.clientChannels.add(channel);
        log.info(" {} 上线", channel.remoteAddress());
    }

    @Override
    public synchronized void channelInactive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        if (LocalCache.clientChannels.contains(channel)) {
            LocalCache.clientChannels.remove(channel);
            log.info("channelInactive : {} 下线", channel.remoteAddress());
        }
    }
}
