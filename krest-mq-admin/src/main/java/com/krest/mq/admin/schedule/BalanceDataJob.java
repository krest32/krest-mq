package com.krest.mq.admin.schedule;

import com.krest.mq.admin.balancer.BrokerBalancer;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.enums.ClusterRole;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class BalanceDataJob {

    /**
     * 被动同步数据的定时任务
     */
    @Async("asyncPool")
    @Scheduled(cron = "0/30 * * * * ?")
    public void reBalanceQueue() {
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {

            if (AdminServerCache.isSelectServer
                    || AdminServerCache.isKidBalanced
                    || AdminServerCache.isSyncData
                    || AdminServerCache.isDetectFollower) {
                return;
            }

            log.info("reBalanceQueue....");
            BrokerBalancer.run();
            log.info("reBalanceQueue finish");
        }
    }
}