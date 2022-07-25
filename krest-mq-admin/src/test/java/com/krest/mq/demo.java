package com.krest.mq;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class demo {

    public static void main(String[] args) throws InterruptedException {
        BlockingDeque<Integer> blockingDeque = new LinkedBlockingDeque<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            TesRunnable runnable = new TesRunnable(blockingDeque, i, countDownLatch);
            Thread t = new Thread(runnable);
            t.start();
        }

        countDownLatch.await();
        System.out.println(blockingDeque.size());

    }
}
