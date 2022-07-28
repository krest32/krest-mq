package com.krest.mq;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class demo {

    public static void main(String[] args) throws InterruptedException {
        BlockingDeque<Integer> blockingDeque = new LinkedBlockingDeque<>();
        blockingDeque.put(1);
        blockingDeque.put(2);
        blockingDeque.put(3);
        blockingDeque.put(4);

        System.out.println(blockingDeque);

        blockingDeque.remove(2);
        System.out.println(blockingDeque);
    }
}
