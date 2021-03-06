package com.krest.mq.core.handler;

import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.runnable.TcpRespHandlerRunnable;

public class TcpRespFutureHandler {

    public TcpRespFutureHandler(int threads) {
        for (int i = 0; i < threads; i++) {
            LocalExecutor.RespHandleExecutor.execute(new TcpRespHandlerRunnable());
        }
    }

    // 将结果放入到 map 集合中
    public void register(String id, MQRespFuture respFuture) {
        BrokerLocalCache.respFutureMap.put(id, respFuture);
    }
}
