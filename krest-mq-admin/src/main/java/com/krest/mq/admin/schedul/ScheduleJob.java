package com.krest.mq.admin.schedul;

import com.krest.mq.admin.cache.AdminCache;
import com.krest.mq.admin.properties.MqConfig;
import com.krest.mq.admin.thread.SearchLeaderRunnable;
import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.core.entity.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;
import com.krest.mq.core.exeutor.LocalExecutor;
import com.krest.mq.core.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Slf4j
@Component
public class ScheduleJob {

    @Autowired
    MqConfig mqConfig;

    @Autowired
    ClusterUtil clusterUtil;

    @Scheduled(cron = "0/10 * * * * ?")
    public void detectFollower() {

        if (!clusterUtil.isReady()) {
            return;
        }

        AdminCache.isDetectFollower = true;
        if (AdminCache.clusterRole.equals(ClusterRole.Leader)) {
            log.info("start detect follower at : " + DateUtils.getNowDate());
            log.info("followers : " + AdminCache.curServers);
            Iterator<ServerInfo> iterator = AdminCache.curServers.iterator();
            while (iterator.hasNext()) {
                ServerInfo curServer = iterator.next();
                boolean flag = clusterUtil.detectFollower(curServer.getTargetAddress(),
                        AdminCache.leaderInfo);
                if (!flag) {
                    int tryCnt = 0;
                    while (tryCnt < 3) {
                        boolean reFlag = clusterUtil.detectFollower(curServer.getTargetAddress(),
                                AdminCache.leaderInfo);
                        if (reFlag) {
                            break;
                        }
                        tryCnt++;
                    }
                    log.info("follower disconnected! : " + curServer);
                    AdminCache.curServers.remove(curServer);
                }
            }
        }
        AdminCache.isDetectFollower = false;
    }


    @Scheduled(cron = "0/10 * * * * ?")
    public void detectLeader() {

        if (!clusterUtil.isReady()) {
            return;
        }
        // 如果 leader 的信息为空，那么就开始寻找
        try {
            if (null == AdminCache.leaderInfo) {
                LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(mqConfig));
            }

            if (!AdminCache.clusterRole.equals(ClusterRole.Leader)) {
                long curMillions = System.currentTimeMillis();
                if (AdminCache.expireTime == null) {
                    AdminCache.resetExpireTime();
                }
                if (curMillions > AdminCache.expireTime) {
                    log.info("过长时间, leader 沒有发送探测报文, follower 发起反向探测");
                    log.info("开始检测 leader 信息 : " + AdminCache.leaderInfo);
                    boolean flag = clusterUtil.detectLeader(
                            AdminCache.leaderInfo.getTargetAddress(), AdminCache.leaderInfo);

                    if (flag) {
                        log.info("反向检测 leader 成功, 重置 follower 反向探测超时时间");
                        AdminCache.resetExpireTime();
                    } else {
                        log.info("反向检测 leader 失败, 开始重新选举 leader ");
                        // 重新设置角色类型，并发起选举
                        clusterUtil.initData();
                        LocalExecutor.NormalUseExecutor.execute(new SearchLeaderRunnable(mqConfig));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
