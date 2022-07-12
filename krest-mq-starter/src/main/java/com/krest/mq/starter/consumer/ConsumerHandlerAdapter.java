package com.krest.mq.starter.consumer;

import com.krest.mq.core.listener.ChannelInactiveListener;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.starter.anno.KrestMQListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;


@Slf4j
public class ConsumerHandlerAdapter extends SimpleChannelInboundHandler<MQMessage.MQEntity> {

    ChannelInactiveListener inactiveListener;
    Object bean;

    private ConsumerHandlerAdapter() {
    }

    public ConsumerHandlerAdapter(ChannelInactiveListener inactiveListener, Object bean) {
        this.inactiveListener = inactiveListener;
        this.bean = bean;
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MQMessage.MQEntity response) throws Exception {
        System.out.println("消费者获取信息：" + response);
        Method[] declaredMethods = this.bean.getClass().getDeclaredMethods();
        for (Method method : declaredMethods) {
            if (method.isAnnotationPresent(KrestMQListener.class)) {
                KrestMQListener krestMQListener = method.getAnnotation(KrestMQListener.class);
                String queue = krestMQListener.queue();
                if (queue.equals(response.getFromQueue())) {
                    method.invoke(bean, ctx, response);
                }
            }
        }
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //super.channelInactive(ctx);
        if (inactiveListener != null) {
            inactiveListener.onInactive();
        }
    }
}
