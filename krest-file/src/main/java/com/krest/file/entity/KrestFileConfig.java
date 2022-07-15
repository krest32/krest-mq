package com.krest.file.entity;

public class KrestFileConfig {
    static String suffix = "queue";
    public static final String indexFileName = suffix + ".index";
    public static final String[] indexFileHeader = new String[]
            {"fileId", "filePath", "endId", "fileSize", "createTime", "updateTime"};

    // 文件最大 500 M
    public static Long maxFileSize;
    public static Integer maxFileCount;

}
