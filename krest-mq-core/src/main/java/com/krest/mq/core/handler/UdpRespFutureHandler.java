package com.krest.mq.core.handler;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.runnable.UdpRespHandlerRunnable;

public class UdpRespFutureHandler {

    public UdpRespFutureHandler(int threads) {
        for (int i = 0; i < threads; i++) {
            LocalExecutor.UdpHandleExecutor.execute(new UdpRespHandlerRunnable());
        }
    }

    // 将结果放入到 map 集合中
    public void register(String id, MQRespFuture respFuture) {
        AdminServerCache.respFutureMap.put(id, respFuture);
    }

}
