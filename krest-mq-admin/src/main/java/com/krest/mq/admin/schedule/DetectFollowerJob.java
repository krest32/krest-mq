package com.krest.mq.admin.schedule;

import com.krest.mq.admin.util.ClusterUtil;
import com.krest.mq.admin.util.SyncDataUtils;
import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.enums.ClusterRole;
import com.krest.mq.core.entity.ServerInfo;

import com.krest.mq.core.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Slf4j
@Component
public class DetectFollowerJob {


    /**
     *
     */
    @Async("asyncPool")
    @Scheduled(cron = "0/30 * * * * ?")
    public void detectFollower() {
        if (AdminServerCache.clusterRole.equals(ClusterRole.Leader)) {
            if (AdminServerCache.isDetectFollower || AdminServerCache.isSelectServer) {
                return;
            }
            AdminServerCache.isDetectFollower = true;
            log.info("start detect follower at : " + DateUtils.getNowDate());
            Iterator<ServerInfo> iterator = SyncDataUtils.mqConfig.getServerList().iterator();
            while (iterator.hasNext()) {
                ServerInfo curServer = iterator.next();
                boolean flag = ClusterUtil.detectFollower(curServer.getTargetAddress(),
                        AdminServerCache.leaderInfo);
                if (!flag) {
                    int tryCnt = 0;
                    while (tryCnt < 1) {
                        boolean reFlag = ClusterUtil.detectFollower(curServer.getTargetAddress(),
                                AdminServerCache.leaderInfo);
                        if (reFlag) {
                            break;
                        }
                        tryCnt++;
                    }
                    AdminServerCache.clusterInfo.getCurServers().remove(curServer);
                } else {
                    AdminServerCache.clusterInfo.getCurServers().add(curServer);
                }
            }
        }
        AdminServerCache.isDetectFollower = false;
    }


}