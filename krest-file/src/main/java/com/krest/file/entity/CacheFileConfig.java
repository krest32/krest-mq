package com.krest.file.entity;

public class CacheFileConfig {
    static String suffix = "queue";
    public static final String indexFileName = suffix + ".index";
    public static final String[] indexFileHeader = new String[]
            {"fileId", "filePath", "startId", "fileSize", "createTime", "updateTime"};

    // 文件最大 500 M
    public static final Integer maxFileSize = 2 * 1024;
    public static final Integer maxFileCount = 3;

}
