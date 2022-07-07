package com.krest.consumer.mq;

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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("demo get msg : " + msg);
    }


    @KrestMQListener(queue = "demo1")
    public void channelRead1(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("demo1 get msg : " + msg);
    }

}
