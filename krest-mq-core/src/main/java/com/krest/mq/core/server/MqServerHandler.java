package com.krest.mq.core.server;

import com.krest.mq.core.entity.MQEntity;
import com.krest.mq.core.entity.ModuleType;
import com.krest.mq.core.entity.MsgStatus;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;


import java.util.*;

@Slf4j
@ChannelHandler.Sharable
public class MqServerHandler extends ChannelInboundHandlerAdapter {

    boolean isPushMode = false;

    private MqServerHandler() {
    }

    public MqServerHandler(boolean isPushMode) {
        this.isPushMode = isPushMode;
    }

    static Map<String, Queue<MQEntity>> queueMap = new HashMap<>();

    static Map<String, Channel> ctxMap = new HashMap<>();

    static ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        MQEntity msgEntity = (MQEntity) msg;
        Set<String> queueNames = msgEntity.getQueueName();
        if (queueNames.isEmpty()) {
            log.error("can not find queue info");
            log.error(msgEntity.toString());
            return;
        } else {
            // 根据不同的类型处理消息
            System.out.println("服务端获取消息：" + msgEntity.toString());
            ModuleType moduleType = msgEntity.getModuleType();

            switch (moduleType) {
                case CONSUMER:
                    consumer(ctx, msgEntity);
                    break;
                case PRODUCER:
                    producer(ctx, msgEntity);
                    break;
                default:
                    log.error("unknown module type");
                    break;
            }
        }
    }


    /**
     * 1. 找到对象的 msg queue 放入消息，如果没有就创建新的 msg queue
     * 2. 给生产者返回响应的确认标记
     * 3. 获取对应消费者的ctx，然后推送消息，但是需要判断ctx是否存活状态
     */
    private void producer(ChannelHandlerContext ctx, MQEntity msgEntity) throws InterruptedException {
        // 回复生产者
        MQEntity result = new MQEntity();
        switch (msgEntity.getMsgStatus()) {
            case PRODUCER_CONNECT_SERVER:
                result.setMsgStatus(MsgStatus.PRODUCER_CONNECT_SERVER_SUCCESS);
                break;
            default:
                result.setMsgStatus(MsgStatus.SEND_TO_SERVER_SUCCESS);
                break;
        }
        ctx.writeAndFlush(result);

        // 整理消息一次放入到每个消息队列中
        Set<String> queueNames = msgEntity.getQueueName();
        for (String queueName : queueNames) {
            Queue<MQEntity> queue = queueMap.get(queueName);
            if (queue == null) {
                queue = new LinkedList<>();
                queueMap.put(queueName, queue);
            }
            // 重置 msg entity 的信息
            msgEntity.setQueueName(null);
            msgEntity.setQueue(queueName);
            msgEntity.setMsgStatus(MsgStatus.STAY_IN_SERVER);
            queue.offer(msgEntity);

            // 发送消息
            Channel channel = ctxMap.get(queueName);
            if (channel != null) {
                if (isPushMode) {
                    while (!queue.isEmpty()) {
                        MQEntity curMqEntity = queue.poll();
                        curMqEntity.setMsgCount(queue.size());
                        curMqEntity.setMsgStatus(MsgStatus.SEND_TO_CONSUMER);
                        channel.writeAndFlush(curMqEntity);
                    }
                } else {
                    MQEntity curMqEntity = queue.poll();
                    curMqEntity.setMsgCount(queue.size());
                    curMqEntity.setMsgStatus(MsgStatus.SEND_TO_CONSUMER);
                    channel.writeAndFlush(curMqEntity);
                }
            }
        }
    }

    /**
     * 1。consumer 是否有对应的 msg queue，如果没有就新建
     * 2. server 返回建立链接的信息
     * 3. 如果是推送机制，那么我们就需要主动推送信息
     */
    private void consumer(ChannelHandlerContext ctx, MQEntity msgEntity) {
        // 返回确认信息
        MQEntity mqEntity = new MQEntity();
        if (msgEntity.getMsgStatus().equals(MsgStatus.CONSUMER_CONNECT_SERVER)) {
            mqEntity.setMsgStatus(MsgStatus.CONSUMER_CONNECT_SERVER_SUCCESS);
        } else {
            msgEntity.setMsgStatus(MsgStatus.SEND_TO_CONSUMER);
        }
        ctx.writeAndFlush(mqEntity);

        // 第一步 如果不存在队列就创建
        Set<String> queueNames = msgEntity.getQueueName();
        for (String queueName : queueNames) {
            // 设置 ctx map
            ctxMap.put(queueName, ctx.channel());
            Queue<MQEntity> curQueue = queueMap.get(queueName);
            // 如果不存在队列就进行创建
            if (curQueue == null) {
                curQueue = new LinkedList<>();
                queueMap.put(queueName, curQueue);
            } else {
                // 如果是主动推送模式
                if (isPushMode) {
                    while (!curQueue.isEmpty()) {
                        MQEntity curMsg = curQueue.poll();
                        curMsg.setMsgStatus(MsgStatus.SEND_TO_CONSUMER);
                        curMsg.setMsgCount(curQueue.size());
                        ctx.writeAndFlush(curMsg);
                    }
                }
            }
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Channel channel = ctx.channel();
        log.info("client {} 下线了", channel.remoteAddress());
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        clientChannels.add(channel);
        log.info("[ {} ] 上线", channel.remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        if (clientChannels.contains(channel)) {
            clientChannels.remove(channel);
            log.info("[ {} ] 下线", channel.remoteAddress());
        }
    }
}
