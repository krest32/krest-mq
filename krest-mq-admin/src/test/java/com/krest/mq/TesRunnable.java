package com.krest.mq;

import lombok.SneakyThrows;

import javax.annotation.security.RunAs;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;

public class TesRunnable implements Runnable {
    BlockingDeque<Integer> blockingDeque;
    Integer num;
    CountDownLatch countDownLatch;

    public TesRunnable(BlockingDeque<Integer> blockingDeque, Integer num, CountDownLatch countDownLatch) {
        this.blockingDeque = blockingDeque;
        this.num = num;
        this.countDownLatch = countDownLatch;
    }

    @SneakyThrows
    @Override
    public void run() {
        this.blockingDeque.put(this.num);
        countDownLatch.countDown();
        System.out.println(Thread.currentThread().getName() + " " + this.blockingDeque.size());
    }
}
