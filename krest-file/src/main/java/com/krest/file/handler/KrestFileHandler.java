package com.krest.file.handler;

import com.krest.file.entity.CacheFileConfig;
import com.krest.file.util.CSVTool;
import com.krest.file.util.DateUtils;
import com.krest.file.util.FileReaderUtils;
import com.krest.file.util.FileWriterUtils;
import com.sun.org.apache.xpath.internal.operations.Bool;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class KrestFileHandler {

    /**
     * 保存当个对象信息进入到文件当中，适合小一些的文件类型
     */
    public synchronized static boolean saveObject(String filePath, Object content) {
        return FileWriterUtils.writeObject(filePath, content);
    }

    /**
     * 读取对象的信息
     */
    public synchronized static Object readObject(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            return FileReaderUtils.readObject(filePath);
        }
        return null;
    }

    public synchronized static boolean saveData(String filePath, String contentId, String content) {

        // 先找到索引文件如果没有，进行新建
        String idxFileName = filePath + "\\" + CacheFileConfig.indexFileName;
        String curFile = null;
        File idxFile = new File(idxFileName);
        List<String[]> idContentList = new ArrayList<>();
        if (!idxFile.exists()) {
            idxFile.getParentFile().mkdirs();
            String curFileId = "1";
            String[] idxContent = new String[]
                    {curFileId, filePath + "\\" + curFileId, contentId, "0", DateUtils.getNowDate(), DateUtils.getNowDate()};
            idContentList.add(idxContent);
            // 创建第一个 idx 文件
            CSVTool.write(idxFileName, CacheFileConfig.indexFileHeader, idContentList);
        }

        idContentList.clear();
        // 读取 idx 文件中的信息， 获取最后一行的信息
        List<String> idxText = CSVTool.getText(idxFileName);
        for (int i = 0; i < idxText.size() - 1; i++) {
            String[] tempCsvRowData = idxText.get(i).split(",");
            idContentList.add(tempCsvRowData);
        }
        String[] lastIdxContent = idxText.get(idxText.size() - 1).split(",");
        lastIdxContent[3] = String.valueOf(new File(lastIdxContent[1]).length());
        lastIdxContent[5] = DateUtils.getNowDate();

        curFile = lastIdxContent[1];
        idContentList.add(lastIdxContent);
        // 判断当前文件的大小
        if (Integer.valueOf(lastIdxContent[3]) >= CacheFileConfig.maxFileSize) {
            // 生成下一个文件信息
            int nextId = Integer.valueOf(lastIdxContent[0]) + 1;
            if (nextId >= CacheFileConfig.maxFileCount) {
                nextId = 1;
            }
            String[] newIdxContext = new String[]
                    {String.valueOf(nextId), filePath + "\\" + nextId, contentId, "0",
                            DateUtils.getNowDate(), DateUtils.getNowDate()};

            // 删除可能已经存在的文件
            File nextFile = new File(newIdxContext[1]);
            if (nextFile.exists()) {
                nextFile.delete();
                // 同时删除Csv中记录的数据
                Iterator<String[]> iterator = idContentList.iterator();
                while (iterator.hasNext()) {
                    String[] strings = iterator.next();
                    if (strings[0].equals(newIdxContext[0])) {
                        iterator.remove();
                    }
                }
            }
            idContentList.add(newIdxContext);

            // 记录新的 index 文件
            curFile = newIdxContext[1];
        }
        // 更新 index 信息
        CSVTool.write(idxFileName, CacheFileConfig.indexFileHeader, idContentList);
        System.out.println("写入文件：");
        System.out.println(curFile);
        System.out.println(content);
        return FileWriterUtils.bufferedWriterMethod(curFile, content, true);
    }

    public static boolean saveData(String filePath, String content, boolean append) {
        return FileWriterUtils.bufferedWriterMethod(filePath, content, append);
    }
}
