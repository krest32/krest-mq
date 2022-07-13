package com.krest.mq.core.runnable;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: krest
 * @date: 2021/5/18 18:50
 * @description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ThreadPoolConfig {
    Integer coreSize = 8;
    Integer maxSize = 16;
    Integer keepAliveTime = 3 * 60 * 1000;
    Integer queueSize = 50 * 1000;
}
