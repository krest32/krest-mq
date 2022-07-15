package com.krest.mq.core.cache;

import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.handler.RespFutureHandler;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class LocalCache implements Serializable {

    private static final long serialVersionUID = 1;

    public static ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    // 已经存在的 queued 的信息的集合
    public static ConcurrentHashMap<String, QueueInfo> queueInfoMap;
    // 延时队列集合
    public static ConcurrentHashMap<String, DelayQueue<DelayMessage>> delayQueueMap = new ConcurrentHashMap<>();
    // 普通队列集合
    public static ConcurrentHashMap<String, BlockingDeque<MQMessage.MQEntity>> queueMap = new ConcurrentHashMap<>();
    // queue对应的 channel list
    public static ConcurrentHashMap<String, List<Channel>> queueCtxListMap = new ConcurrentHashMap<>();
    // channel 对应的 queue list
    public static ConcurrentHashMap<Channel, List<String>> ctxQueueListMap = new ConcurrentHashMap<>();
    // 是否为 push 模式
    public static boolean isPushMode = true;
    public static Channel udpChannel;
    // udp 集合
    public static ConcurrentHashMap<String, CopyOnWriteArraySet<InetSocketAddress>> packetQueueMap = new ConcurrentHashMap<>();
    // ack 模式结果处理集合
    public static BlockingDeque<MQMessage.MQEntity> responseQueue = new LinkedBlockingDeque<>();
    // 异步结结 Future 集合
    public static ConcurrentMap<String, MQRespFuture> respFutureMap = new ConcurrentHashMap<>();
    // 异步结果处理器
    public static RespFutureHandler respFutureHandler = new RespFutureHandler(4);
}
