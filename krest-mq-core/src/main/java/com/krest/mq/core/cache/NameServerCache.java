package com.krest.mq.core.cache;

import com.krest.mq.core.entity.*;
import com.krest.mq.core.handler.TcpRespFutureHandler;
import com.krest.mq.core.handler.UdpRespFutureHandler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

public class NameServerCache {
    public static ConcurrentHashMap<String, ClusterRole> clusterRoleMap = new ConcurrentHashMap<>();
    public static RunningMode runningMode = RunningMode.Single;
    public static UdpRespFutureHandler udpRespFutureHandler = new UdpRespFutureHandler(4);
    // 异步结结 Future 集合
    public static ConcurrentMap<String, MQRespFuture> respFutureMap = new ConcurrentHashMap<>();
    // ack 模式结果处理集合
    public static BlockingDeque<MQMessage.MQEntity> responseQueue = new LinkedBlockingDeque<>();
}
