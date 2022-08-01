package com.krest.mq.admin.thread;


import com.krest.mq.core.cache.AdminServerCache;
import com.krest.mq.core.cache.BrokerLocalCache;
import com.krest.mq.core.entity.QueueInfo;
import com.krest.mq.core.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Map;

@Slf4j
public class CheckSyncDataRunnable implements Runnable {

    static final String HTTP_START_STR = "http://";
    static final String NOTIFY_LEADER_PATH = "/mq/manager/notify/leader/sync/data/finish";

    @Override
    public void run() {
        // 检查当前的 server 是否已经完成了数据的同步

        while (true) {
            if (checkSyncDataStatus()) {
                break;
            }
            try {
                log.info("still in sync data.....");
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }

        // 通知 leader 同步数据完成, 如果 leader 不存在放弃
        if (!AdminServerCache.isSelectServer && null != AdminServerCache.leaderInfo) {
            String notifyLeaderPath = HTTP_START_STR + AdminServerCache.leaderInfo.getTargetAddress() + NOTIFY_LEADER_PATH;
            String postRequest = HttpUtil.postRequest(notifyLeaderPath, AdminServerCache.kid);
            AdminServerCache.isSyncData = false;
            while (StringUtils.isBlank(postRequest) || !postRequest.equals("1")) {
                String reResponse = HttpUtil.postRequest(notifyLeaderPath, AdminServerCache.kid);
                if (!StringUtils.isBlank(reResponse)) {
                    if (reResponse.equals("1")) {
                        log.info("notify leader success");
                        return;
                    }
                    if (reResponse.equals("-1")) {
                        log.info("notify leader failed , retry...... ");
                    } else {
                        log.error("happen error， response msg : {} ", reResponse);
                    }
                }
                try {
                    Thread.sleep(3 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            log.info("notify leader success");
        }
    }

    private boolean checkSyncDataStatus() {
        Iterator<Map.Entry<String, QueueInfo>> iterator = AdminServerCache.syncTargetQueueInfoMap
                .entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueInfo> next = iterator.next();
            String queueName = next.getKey();
            QueueInfo targetQueueInfo = next.getValue();
            QueueInfo curQueueInfo = BrokerLocalCache.queueInfoMap.get(queueName);
            if (null != curQueueInfo
                    && null != targetQueueInfo.getAmount()
                    && null != curQueueInfo.getAmount()
                    && null != targetQueueInfo.getOffset()
                    && null != curQueueInfo.getOffset()) {
                if (!targetQueueInfo.getAmount().equals(curQueueInfo.getAmount())
                        || !curQueueInfo.getOffset().equals(targetQueueInfo.getOffset())) {
                    return false;
                }
            }
        }

        return true;
    }
}
