package com.krest.mq.core.entity;

import lombok.Data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Data
public class ClusterInfo {
    // 集群中每个 queue 的副本数量
    Integer duplicate;
    // 系统保存的，存活状态的 server， 如果有的 server 长时间未收到 leader 信息，会反向探测
    Set<ServerInfo> curServers = new CopyOnWriteArraySet<>();
    // 激励每个 queue 对应的大小
    Map<String, Integer> queueSizeMap = new ConcurrentHashMap<>();
    // 记录每个 queue 对应的最新的数据的 kid
    Map<String, String> queueLatestKid = new ConcurrentHashMap<>();
    // 记录每个队列的最新 offset
    Map<String, Long> queueOffsetMap = new ConcurrentHashMap<>();
    // 集群中每个 queue 的名称对应 对量 的 map
    Map<String, Integer> queueAmountMap = new ConcurrentHashMap<>();
    // 每个 kid 对应的 queue info map
    Map<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfo = new ConcurrentHashMap<>();
    // 每个 queue 对应的 pocket 信息
    Map<String, Set<ServerInfo>> queuePacketMap = new ConcurrentHashMap<>();
    // 每个 kid 的当前状态
    Map<String, Integer> kidStatusMap = new ConcurrentHashMap<>();

}
