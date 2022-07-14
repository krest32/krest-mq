package com.krest.consumer.mq;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.starter.anno.KrestConsumer;
import com.krest.mq.starter.anno.KrestMQListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KrestConsumer
public class MQListener {

    @KrestMQListener(queue = "demo")
    public void channelRead(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
        log.info("demo get msg : " + response.getMsg());
    }


    @KrestMQListener(queue = "demo1")
    public void channelRead1(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
        log.info("demo1 get msg : " + response.getMsg());
    }

}
