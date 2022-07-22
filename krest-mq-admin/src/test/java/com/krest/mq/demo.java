package com.krest.mq;

import java.util.*;

public class demo {

    public static void main(String[] args) {
        Map<String, Integer> map = new HashMap<>();
        PriorityQueue<Map.Entry<String, Integer>> priorityQueue = new PriorityQueue<>(
                (o1, o2) -> o1.getValue() - o2.getValue());

        map.put("a", 1);
        map.put("b", 4);
        map.put("c", 2);
        map.put("f", 2);
        map.put("e", 3);
        map.put("d", 5);
        Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> next = iterator.next();
            map.put("d", 15);
            map.put("e", 13);
            System.out.println(next);
            priorityQueue.add(next);
        }
//
//        while (!priorityQueue.isEmpty()) {
//            System.out.println(priorityQueue.poll());
//        }

    }
}
