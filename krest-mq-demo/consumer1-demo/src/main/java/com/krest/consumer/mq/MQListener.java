package com.krest.consumer.mq;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.enums.QueueType;
import com.krest.mq.starter.anno.KrestConsumer;
import com.krest.mq.starter.anno.KrestMQListener;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KrestConsumer
public class MQListener {

    int cnt = 0;

    @KrestMQListener(queue = "demo", queueType = QueueType.PERMANENT)
    public void channelRead(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
//        log.info("demo get msg : " + response.getMsg());
        // 如果是 Ack 模式, 则返回一个 确认的 id 信息
        log.info(response.getId());
        if (response.getIsAck()) {
//            Thread.sleep(1000);
            ctx.writeAndFlush(MQMessage.MQEntity.newBuilder()
                    .setId(response.getId())
                    .setMsgType(3)
                    .build());
        }
    }

    @KrestMQListener(queue = "demo1", queueType = QueueType.TEMPORARY)
    public void channelRead1(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
//        log.info("demo1 get msg : " + response.getMsg());

        if (response.getIsAck()) {
//            Thread.sleep(1000);
            ctx.writeAndFlush(MQMessage.MQEntity.newBuilder()
                    .setId(response.getId())
                    .setMsgType(3).build());
        }
        cnt++;
        if (cnt==9_999){
            System.out.println(cnt);
        }
    }

    @KrestMQListener(queue = "demo2", queueType = QueueType.DELAY)
    public void channelRead2(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
        log.info("demo2 get msg : " + response.getMsg());
        if (response.getIsAck()) {
//            Thread.sleep(1000);
            ctx.writeAndFlush(MQMessage.MQEntity.newBuilder()
                    .setId(response.getId())
                    .setMsgType(3).build());
        }
    }

    @KrestMQListener(queue = "demo3", queueType = QueueType.DELAY)
    public void channelRead3(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
        log.info("demo3 get msg : " + response.getMsg());
        if (response.getIsAck()) {
//            Thread.sleep(1000);
            ctx.writeAndFlush(MQMessage.MQEntity.newBuilder()
                    .setId(response.getId())
                    .setMsgType(3).build());
        }
    }
//
//    @KrestMQListener(queue = "demo4", queueType = QueueType.DELAY)
//    public void channelRead4(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
//        log.info("demo3 get msg : " + response.getMsg());
//        if (response.getIsAck()) {
//            Thread.sleep(1000);
//            ctx.writeAndFlush(MQMessage.MQEntity.newBuilder()
//                    .setId(response.getId())
//                    .setMsgType(3).build());
//        }
//    }
}
