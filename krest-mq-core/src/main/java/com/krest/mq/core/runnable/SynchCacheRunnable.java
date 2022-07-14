package com.krest.mq.core.runnable;

import com.krest.file.handler.KrestFileHandler;
import com.krest.mq.core.cache.CacheFileConfig;
import com.krest.mq.core.cache.LocalCache;

public class SynchCacheRunnable implements Runnable {
    @Override
    public void run() {
        // 同步 queue info
        KrestFileHandler.saveObject(CacheFileConfig.queueInfoFilePath, LocalCache.queueInfoMap);
    }
}
