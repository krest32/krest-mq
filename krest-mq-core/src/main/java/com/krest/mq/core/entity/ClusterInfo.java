package com.krest.mq.core.entity;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

@Data
public class ClusterInfo {
    // 集群中每个 queue 的副本数量
    Integer duplicate;
    // 激励每个 queue 对应的大小
    ConcurrentHashMap<String, Integer> queueSizeMap = new ConcurrentHashMap<>();
    // 记录每个 queue 对应的最新的数据的 kid
    ConcurrentHashMap<String, String> queueLatestKid = new ConcurrentHashMap<>();
    // 记录每个队列的最新 offset
    ConcurrentHashMap<String, Long> queueOffsetMap = new ConcurrentHashMap<>();
    // 集群中每个 queue 的名称对应 对量 的 map
    ConcurrentHashMap<String, Integer> queueAmountMap = new ConcurrentHashMap<>();
    // 每个 kid 对应的 queue info map
    ConcurrentHashMap<String, ConcurrentHashMap<String, QueueInfo>> kidQueueInfo = new ConcurrentHashMap<>();

}
