package com.krest.mq.admin.schedule;

import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;

import com.krest.mq.core.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Slf4j
@Component
public class DetectFollowerJob {

    @Async("asyncPool")
    @Scheduled(cron = "0/30 * * * * ?")
    public void detectFollower() {
        if (AdminServerCache.clusterRole.equals(ClusterRole.LEADER)) {
            if (AdminServerCache.isDetectFollower
                    || AdminServerCache.isSelectServer
                    || AdminServerCache.isSyncClusterInfo) {
                return;
            }

            log.info("start detect follower at : " + DateUtils.getNowDate());
            Iterator<ServerInfo> iterator = AdminServerCache.clusterInfo.get()
                    .getCurServers().iterator();
            while (iterator.hasNext()) {
                ServerInfo curServer = iterator.next();
                boolean flag = ClusterUtil.detectFollower(curServer.getTargetAddress(),
                        AdminServerCache.leaderInfo);
                if (!flag) {
                    // 如果检测过程中发现异常，那么既不能进行 balancer 工作
                    AdminServerCache.isDetectFollower = true;
                    int tryCnt = 1;
                    while (tryCnt < 3) {
                        boolean reFlag = ClusterUtil.detectFollower(curServer.getTargetAddress(),
                                AdminServerCache.leaderInfo);
                        if (reFlag) {
                            break;
                        }
                        tryCnt++;
                    }
                    AdminServerCache.clusterInfo.get().getCurServers().remove(curServer);
                } else {
                    AdminServerCache.clusterInfo.get().getCurServers().add(curServer);
                }
            }
        }
        AdminServerCache.isDetectFollower = false;
    }


}