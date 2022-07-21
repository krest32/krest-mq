package com.krest.mq.core.entity;

import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Data
public class ClusterInfo {
    // 集群中每个 queue 的副本数量
    Integer duplicate;
    // 集群中每个 queue kid 的名称对应的 map
    ConcurrentHashMap<String, CopyOnWriteArraySet<String>> queueKidMap;
    // 集群中每个副本的数量
    ConcurrentHashMap<String, Integer> queueAmountMap;

}
