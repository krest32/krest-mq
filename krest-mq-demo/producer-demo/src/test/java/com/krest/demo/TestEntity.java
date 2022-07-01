package com.krest.demo;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class TestEntity  extends ChannelInboundHandlerAdapter implements InvocationHandler  {
    int a;
    EntityInactiveHandler entityInactiveHandler;

    public TestEntity(int a) {
        this.a = a;
        entityInactiveHandler = () -> {
            do {
                System.out.println("监听器执行");
                Thread.sleep(1000);
            } while (this.a == 0);
        };
    }

    public int getA() {
        System.out.println(a);
        return a;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("执行代理方法");
        return null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
    }
}
