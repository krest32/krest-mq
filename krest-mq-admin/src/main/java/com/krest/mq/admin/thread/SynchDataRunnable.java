package com.krest.mq.admin.thread;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.ServerInfo;

import java.util.concurrent.Callable;

public class SynchDataRunnable implements Callable {

    String from;
    String to;
    String queueName;

    public SynchDataRunnable(String from, String to, String queueName) {
        this.from = from;
        this.to = to;
        this.queueName = queueName;
    }

    @Override
    public Object[] call() throws Exception {
        Object[] ans = new Object[2];
        ans[0] = this.to;
        ServerInfo fromServer = AdminServerCache.kidServerMap.get(this.from);
        ServerInfo toServer = AdminServerCache.kidServerMap.get(this.to);

        // 通过 http 发送指令




        return new Object[2];
    }
}
