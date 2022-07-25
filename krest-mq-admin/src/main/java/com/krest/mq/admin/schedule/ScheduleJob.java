package com.krest.mq.admin.schedule;

import com.krest.mq.admin.balancer.BrokerBalancer;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Slf4j
@Component
public class ScheduleJob {

    @Autowired
    ClusterUtil clusterUtil;

    @Scheduled(cron = "0/30 * * * * ?")
    public void detectFollower() {

        if (!SyncDataUtils.isClusterReady())
            return;

        AdminServerCache.isDetectFollower = true;

        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("start detect follower at : " + DateUtils.getNowDate());
            log.info("followers : " + AdminServerCache.curServers);
            Iterator<ServerInfo> iterator = AdminServerCache.curServers.iterator();
            while (iterator.hasNext()) {
                ServerInfo curServer = iterator.next();
                boolean flag = clusterUtil.detectFollower(curServer.getTargetAddress(),
                        AdminServerCache.leaderInfo);
                if (!flag) {
                    int tryCnt = 0;
                    while (tryCnt < 3) {
                        boolean reFlag = clusterUtil.detectFollower(curServer.getTargetAddress(),
                                AdminServerCache.leaderInfo);
                        if (reFlag) {
                            break;
                        }
                        tryCnt++;
                    }
                    log.info("follower disconnected! : " + curServer);
                    AdminServerCache.curServers.remove(curServer);
                }
            }
        }

        AdminServerCache.isDetectFollower = false;
    }


    @Scheduled(cron = "0/30 * * * * ?")
    public void detectLeader() {

        if (!SyncDataUtils.isClusterReady())
            return;

        // 如果 leader 的信息为空，那么就开始寻找
        try {
            if (null == AdminServerCache.leaderInfo) {
                LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(SyncDataUtils.mqConfig));
            }

            if (!AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
                long curMillions = System.currentTimeMillis();
                if (AdminServerCache.expireTime == null) {
                    AdminServerCache.resetExpireTime();
                }
                if (curMillions > AdminServerCache.expireTime) {
                    log.info("沒有收到探测报文, follower 反向探测 leader 信息, Leader : " + AdminServerCache.leaderInfo);
                    boolean flag = clusterUtil.detectLeader(
                            AdminServerCache.leaderInfo.getTargetAddress(), AdminServerCache.leaderInfo);
                    if (flag) {
                        log.info("反向检测 leader 成功, 重置 follower 反向探测超时时间, 并重新注册自己");
                        String ans = clusterUtil.registerSelf();
                        if (StringUtils.isBlank(ans)) {
                            log.info("可能存在多个 leader, 开始重新选举");
                            clusterUtil.initData();
                            LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(SyncDataUtils.mqConfig));
                        } else {
                            AdminServerCache.resetExpireTime();
                        }
                    } else {
                        // 重新设置角色类型，并发起选举
                        log.info("反向检测 leader 失败, 开始重新选举 leader ");

                        clusterUtil.initData();
                        LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(SyncDataUtils.mqConfig));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 被动同步数据的定时任务
     */
    @Scheduled(cron = "0/30 * * * * ?")
    public void reBalanceQueue() {
        // 只有整个集群 ok 的情况下， 才会进行数据同步任务
        if (!SyncDataUtils.isClusterReady())
            return;

        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            SyncDataUtils.syncClusterInfo();
            BrokerBalancer.run();
        }
    }
}