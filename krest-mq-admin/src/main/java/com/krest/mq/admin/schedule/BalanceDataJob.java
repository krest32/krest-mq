package com.krest.mq.admin.schedule;

import com.krest.mq.admin.balancer.BrokerBalancer;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.enums.ClusterRole;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

            if (AdminServerCache.isSelectServer)
                return;

            if (AdminServerCache.isKidBalanced)
                return;

            if (AdminServerCache.isSyncData)
                return;
            log.info("reBalanceQueue....");

            SyncDataUtils.syncClusterInfo();

            BrokerBalancer.run();

            log.info("reBalanceQueue finish");
        }
    }
}