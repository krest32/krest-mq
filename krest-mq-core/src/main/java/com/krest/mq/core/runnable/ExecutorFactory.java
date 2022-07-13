package com.krest.mq.core.runnable;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorFactory {

    public static ThreadPoolExecutor threadPoolExecutor(ThreadPoolConfig config) {
        return new ThreadPoolExecutor(
                config.coreSize,
                config.maxSize,
                config.keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(config.getQueueSize()),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
