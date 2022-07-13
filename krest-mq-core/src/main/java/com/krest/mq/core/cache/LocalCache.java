package com.krest.mq.core.cache;

import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.QueueInfo;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.Data;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Data
public class LocalCache {

    public static Channel udpChannel;

    public static boolean isPushMode = true;

    public static ConcurrentHashMap<String, CopyOnWriteArraySet<InetSocketAddress>> packetQueueMap = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, QueueInfo> queueInfoMap = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, BlockingQueue<MQMessage.MQEntity>> queueMap = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, List<Channel>> ctxMap = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<Channel, List<String>> ctxQueueMap = new ConcurrentHashMap<>();

    public static ChannelGroup clientChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
}
