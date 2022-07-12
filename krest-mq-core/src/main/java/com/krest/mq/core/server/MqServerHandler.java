package com.krest.mq.core.server;

import com.google.protobuf.ProtocolStringList;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.utils.DateUtils;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class MqServerHandler extends SimpleChannelInboundHandler<MQMessage.MQEntity> {

    boolean isPushMode = false;

    private MqServerHandler() {
    }

    public MqServerHandler(boolean isPushMode) {
        this.isPushMode = isPushMode;
    }

    static Map<String, Queue<MQMessage.MQEntity>> queueMap = new HashMap<>();

    static Map<String, Channel> ctxMap = new HashMap<>();

    static ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MQMessage.MQEntity request) throws Exception {
        ProtocolStringList queueList = request.getToQueueList();
        if (queueList.isEmpty()) {
            log.error("can not find queue info");
            return;
        } else {
            // 根据不同的类型处理消息
            System.out.println("服务端获取消息：" + request.toString());
            int msgType = request.getMsgType();
            switch (msgType) {
                // 0 代表消费者
                case 0:
                    consumer(ctx, request);
                    break;
                // 1 代表生产则
                case 1:
                    producer(ctx, request);
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
    private void producer(ChannelHandlerContext ctx, MQMessage.MQEntity request) throws InterruptedException {
        // 回复生产者
        if (request.getIsAck()) {
            MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
            MQMessage.MQEntity response = builder.setId(request.getId())
                    .setAck(true)
                    .setDateTime(DateUtils.getNowDate())
                    .build();
            System.out.println("返回确认消息");
            System.out.println(response);
            ctx.writeAndFlush(response);
        }

        // 整理消息一次放入到每个消息队列中
        ProtocolStringList queueNames = request.getToQueueList();
        if (!queueNames.isEmpty()) {
            for (String queueName : queueNames) {
                Queue<MQMessage.MQEntity> queue = queueMap.get(queueName);
                if (queue == null) {
                    queue = new LinkedList<>();
                    queueMap.put(queueName, queue);
                }
                // 重置 msg entity 的信息
                MQMessage.MQEntity.Builder builder = MQMessage.MQEntity.newBuilder();
                MQMessage.MQEntity response = builder.setId(request.getId())
                        .setMsg(request.getMsg())
                        .setAck(true)
                        .setDateTime(DateUtils.getNowDate())
                        .setFromQueue(queueName)
                        .build();
                queue.offer(response);
                // 发送消息
                Channel channel = ctxMap.get(queueName);
                if (channel != null) {
                    if (isPushMode) {
                        while (!queue.isEmpty()) {
                            channel.writeAndFlush(queue.poll());
                        }
                    } else {
                        channel.writeAndFlush(queue.poll());
                    }
                }
            }
        }
    }

    /**
     * 1。consumer 是否有对应的 msg queue，如果没有就新建
     * 2. server 返回建立链接的信息
     * 3. 如果是推送机制，那么我们就需要主动推送信息
     */
    private void consumer(ChannelHandlerContext ctx, MQMessage.MQEntity request) {
        // 返回确认信息
        if (request.getIsAck()) {
            MQMessage.MQEntity.Builder response = MQMessage.MQEntity.newBuilder();
            response.setId(request.getId())
                    .setAck(true)
                    .setDateTime(DateUtils.getNowDate())
                    .build();
            ctx.writeAndFlush(response);
        }

        // 第一步 如果不存在队列就创建
        ProtocolStringList queueList = request.getToQueueList();
        if (!queueList.isEmpty()) {
            for (String queueName : queueList) {
                // 设置 ctx map
                ctxMap.put(queueName, ctx.channel());
                Queue<MQMessage.MQEntity> curQueue = queueMap.get(queueName);
                // 如果不存在队列就进行创建
                if (curQueue == null) {
                    curQueue = new LinkedList<>();
                    queueMap.put(queueName, curQueue);
                } else {
                    // 如果是主动推送模式
                    if (isPushMode) {
                        while (!curQueue.isEmpty()) {
                            ctx.writeAndFlush(curQueue.poll());
                        }
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
