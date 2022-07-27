package com.krest.mq;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class demo {

    public static void main(String[] args)  {
        Map<String,Integer> map = new HashMap<>();
        Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()){
            System.out.println("haha");
            System.out.println(iterator);
        }

    }
}
