package com.krest.mq.core.cache;

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
    // 已经存在的 queue， 不允许修改属性 临时或者持久属性
    public static ConcurrentHashMap<String, QueueInfo> queueInfoMap;
    public static ConcurrentHashMap<String, BlockingDeque<MQMessage.MQEntity>> queueMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, List<Channel>> queueCtxListMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Channel, List<String>> ctxQueueListMap = new ConcurrentHashMap<>();
    public static boolean isPushMode = true;
    public static Channel udpChannel;
    public static ConcurrentHashMap<String, CopyOnWriteArraySet<InetSocketAddress>> packetQueueMap = new ConcurrentHashMap<>();

    public static BlockingDeque<MQMessage.MQEntity> responseQueue = new LinkedBlockingDeque<>();
    public static ConcurrentMap<String, MQRespFuture> respFutureMap = new ConcurrentHashMap<>();

    public static RespFutureHandler respFutureHandler = new RespFutureHandler(4);
}
