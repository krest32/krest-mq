package com.krest.mq.core.handler;
import com.krest.mq.core.cache.BrokerLocalCache;
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
        BrokerLocalCache.clientChannels.remove(channel);
        // 更新本地缓存的 channel 信息
        List<String> queueList = BrokerLocalCache.ctxQueueListMap.getOrDefault(channel, null);
        if (null != queueList) {
            for (String queueName : queueList) {
                List<Channel> channels = BrokerLocalCache.queueCtxListMap.get(queueName);
                channels.remove(channel);
                BrokerLocalCache.queueCtxListMap.put(queueName, channels);
            }
        }
        BrokerLocalCache.ctxQueueListMap.remove(channel);
        log.info("exceptionCaught client {} 下线了", channel.remoteAddress());
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        BrokerLocalCache.clientChannels.add(channel);
        log.info(" {} 上线", channel.remoteAddress());
    }

    @Override
    public synchronized void channelInactive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        if (BrokerLocalCache.clientChannels.contains(channel)) {
            BrokerLocalCache.clientChannels.remove(channel);
            log.info("channelInactive : {} 下线", channel.remoteAddress());
        }
    }
}
