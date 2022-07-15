package com.krest.mq.core.entity;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
@ToString
public class MQRespFuture {

    // 远程调用的结果类型
    public final static int STATE_AWAIT = 0;
    public final static int STATE_SUCCESS = 1;

    // 等待调用结果
    private CountDownLatch countDownLatch;

    int timeout;
    int state;
    String msgId;


    public MQRespFuture(String msgId, int timeout) {
        this.msgId = msgId;
        countDownLatch = new CountDownLatch(1);
        state = STATE_AWAIT;
    }

    /**
     * 设置结果
     */
    public void setResult() {
        state = STATE_SUCCESS;
        countDownLatch.countDown();
    }

    public String get() throws Throwable {
        // 等待结果
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        if (state != STATE_AWAIT) {
            return this.msgId;
        } else {
            throw new RuntimeException(this.msgId + " : return error");
        }
    }

    /**
     * 等待多长时间然后获取结果
     */
    public String get(int timeout) throws Throwable {
        boolean awaitSuccess = true;

        try {
            awaitSuccess = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        if (!awaitSuccess) {
            throw new RuntimeException();
        }

        if (state == STATE_SUCCESS) {
            return this.msgId;
        } else {
            throw new RuntimeException(this.msgId + " : return error");
        }
    }

    /**
     * 判断当前任务是否处理完成
     */
    public boolean isDone() {
        return state != STATE_AWAIT;
    }

}
