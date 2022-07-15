package com.krest.mq.core.runnable;

import com.krest.mq.core.cache.LocalCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TcpRespHandlerRunnable implements Runnable {

    @Override
    public void run() {
        while (true) {
            try {
                // 从队列中获取请求结果
                MQMessage.MQEntity mqResp = LocalCache.responseQueue.take();
                // 得到结果
                MQRespFuture respFuture = LocalCache.respFutureMap.remove(mqResp.getId());
                // 将调用的结果放入到 Future 调用的结果当中
                respFuture.setResult();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
