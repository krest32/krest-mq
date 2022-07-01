package com.krest.mq.starter.consumer;

import com.krest.mq.core.entity.ChannelInactiveListener;
import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.starter.anno.KrestMQListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.Set;


@Slf4j
@ChannelHandler.Sharable
public class ConsumerHandlerAdapter extends ChannelInboundHandlerAdapter {

    ChannelInactiveListener inactiveListener;
    Object bean;

    private ConsumerHandlerAdapter() {
    }

    public ConsumerHandlerAdapter(ChannelInactiveListener inactiveListener, Object bean) {
        this.inactiveListener = inactiveListener;
        this.bean = bean;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("消费者获取信息：" + msg);
        MQEntity entity = (MQEntity) msg;
        Method[] declaredMethods = this.bean.getClass().getDeclaredMethods();
        for (Method method : declaredMethods) {
            if (method.isAnnotationPresent(KrestMQListener.class)) {
                KrestMQListener krestMQListener = method.getAnnotation(KrestMQListener.class);
                String queue = krestMQListener.queue();
                if (queue.equals(entity.getQueue())) {
                    method.invoke(bean, ctx, entity.getMsg());
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
