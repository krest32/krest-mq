package com.krest.mq.dely;

import java.util.concurrent.DelayQueue;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        DelayQueue<DelayTask> delayQueue = new DelayQueue<>();
        long cur = System.currentTimeMillis();
        long[] delay = {15000L, 10000L, 5000L, 20000L};
        for (int i = 0; i < delay.length; i++) {
            DelayTask task = new DelayTask(delay[i] + cur, i);
            delayQueue.add(task);
        }
        while (!delayQueue.isEmpty()) {
            DelayTask delayTask = delayQueue.take();
            System.out.println("当前任务序号：" + delayTask.index);
            Thread.sleep(1000);
            delayQueue.offer(delayTask);
        }
    }
}
