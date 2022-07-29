package com.krest.mq.admin.schedule;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class ScheduleConfig  implements AsyncConfigurer {

    @Bean("asyncPool")
    public Executor asyncPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        /** Set the ThreadPoolExecutor's core pool size. */
        int corePoolSize = 10;
        executor.setCorePoolSize(corePoolSize);
        /** Set the ThreadPoolExecutor's maximum pool size. */
        int maxPoolSize = 50;
        executor.setMaxPoolSize(maxPoolSize);
        /** Set the capacity for the ThreadPoolExecutor's BlockingQueue. */
        int queueCapacity = 200;

        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("schedule-pool-");

        // rejection-policy：当pool已经达到max size的时候，如何处理新任务
        // CALLER_RUNS：不在新线程中执行任务，而是有调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
