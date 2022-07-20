package com.krest.mq.core.exeutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

public class LocalExecutor {

    public static ThreadPoolExecutor TcpExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

    public static ThreadPoolExecutor RespHandleExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

    public static ThreadPoolExecutor TcpDelayExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

    public static ThreadPoolExecutor UdpHandleExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

    public static ThreadPoolExecutor NormalUseExecutor = ExecutorFactory.threadPoolExecutor(new ThreadPoolConfig());

}
