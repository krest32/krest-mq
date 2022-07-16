package com.krest.mq.core.handler;
import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.processor.TcpServerMsgProcessor;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
public class MqTcpServerHandler extends SimpleChannelInboundHandler<MQMessage.MQEntity> {


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MQMessage.MQEntity request) {
        TcpServerMsgProcessor.msgCenter(ctx, request);
    }


    @Override
    public synchronized void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Channel channel = ctx.channel();
        LocalCache.clientChannels.remove(channel);
        // 更新本地缓存的 channel 信息
        List<String> queueList = LocalCache.ctxQueueListMap.getOrDefault(channel, null);
        if (null != queueList) {
            for (String queueName : queueList) {
                List<Channel> channels = LocalCache.queueCtxListMap.get(queueName);
                channels.remove(channel);
                LocalCache.queueCtxListMap.put(queueName, channels);
            }
        }
        LocalCache.ctxQueueListMap.remove(channel);
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
