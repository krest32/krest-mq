package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.BrokerLocalCache;

public class SynchLocalDataRunnable implements Runnable {
    @Override
    public void run() {
        // 同步 queue info
        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, BrokerLocalCache.queueInfoMap);
    }
}
