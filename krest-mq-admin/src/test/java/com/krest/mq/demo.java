package com.krest.mq;

import com.alibaba.fastjson.JSONObject;
import com.krest.mq.core.entity.QueueInfo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class demo {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Integer> map1 = new HashMap<>();
        map1.put("1", 1);
        map1.put("2", 1);
        Map<String, Integer> map2 = new ConcurrentHashMap<>();
        map2.put("2", 1);
        map2.put("1", 1);
        System.out.println(map1.equals(map2));
    }
}
