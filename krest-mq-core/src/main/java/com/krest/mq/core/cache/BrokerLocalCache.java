package com.krest.mq.core.cache;

import com.krest.mq.core.entity.DelayMessage;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.handler.TcpRespFutureHandler;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class BrokerLocalCache implements Serializable, Cloneable {

    private static final long serialVersionUID = 1;


    public static ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    // 已经存在的 queued 的信息的集合
    public static Map<String, QueueInfo> queueInfoMap = new ConcurrentHashMap<>();
    // 延时队列集合
    public static Map<String, DelayQueue<DelayMessage>> delayQueueMap = new ConcurrentHashMap<>();
    // 普通队列集合
    public static Map<String, BlockingDeque<MQMessage.MQEntity>> queueMap = new ConcurrentHashMap<>();
    // queue对应的 channel list
    public static Map<String, List<Channel>> queueCtxListMap = new ConcurrentHashMap<>();
    // channel 对应的 queue list
    public static Map<Channel, List<String>> ctxQueueListMap = new ConcurrentHashMap<>();

    public static Channel udpChannel;
    // udp 集合
    public static Map<String, CopyOnWriteArraySet<InetSocketAddress>> packetQueueMap = new ConcurrentHashMap<>();
    // ack 模式结果处理集合
    public static BlockingDeque<MQMessage.MQEntity> responseQueue = new LinkedBlockingDeque<>();
    // 异步结结 Future 集合
    public static Map<String, MQRespFuture> respFutureMap = new ConcurrentHashMap<>();
    // 异步结果处理器
    public static TcpRespFutureHandler tcpRespFutureHandler = new TcpRespFutureHandler(4);
}
