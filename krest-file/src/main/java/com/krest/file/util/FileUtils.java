package com.krest.file.util;

import java.io.*;

public class FileUtils {

    /**
     * 方法 2：使用 BufferedWriter 写文件
     *
     * @param filepath 文件目录
     * @param content  待写入内容
     * @throws IOException
     */
    public static void bufferedWriterMethod(String filepath, String content, boolean append) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath, append))) {
            bufferedWriter.write(content);
        }
    }

    /**
     * 方法 5：使用 BufferedOutputStream 写文件
     *
     * @param filepath 文件目录
     * @param content  待写入内容
     * @throws IOException
     */
    public static void bufferedOutputStreamMethod(String filepath, byte[] content, boolean append) throws IOException {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                new FileOutputStream(filepath, append))) {
            bufferedOutputStream.write(content);
        }
    }
}
