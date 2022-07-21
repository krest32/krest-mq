package com.krest.mq.core.runnable;

import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.entity.MQMessage;
import com.krest.mq.core.entity.MQRespFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * 用作处理 Udp Ack 的返回数据的
 */
@Slf4j
public class UdpRespHandlerRunnable implements Runnable {

    @Override
    public void run() {
        while (true) {
            try {
                // 从队列中获取请求结果
                MQMessage.MQEntity mqResp = AdminServerCache.responseQueue.take();
                // 得到结果
                MQRespFuture respFuture = AdminServerCache.respFutureMap.remove(mqResp.getId());
                // 将调用的结果放入到 Future 调用的结果当中
                if (null != respFuture) {
                    System.out.println(Thread.currentThread().getName() + " : 找到 Future : " + mqResp.getId());
                    respFuture.setResult();
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
