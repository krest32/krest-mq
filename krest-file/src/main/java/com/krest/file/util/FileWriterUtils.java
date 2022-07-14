package com.krest.file.util;

import java.io.*;

public class FileWriterUtils {

    /**
     * 方法 2：使用 BufferedWriter 写文件
     *
     * @param filepath 文件目录
     * @param content  待写入内容
     * @throws IOException
     */
    public static boolean bufferedWriterMethod(String filepath, String content, boolean append) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath, append))) {
            bufferedWriter.write(content);
            bufferedWriter.write("\n");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 方法 5：使用 BufferedOutputStream 写文件
     *
     * @param filepath 文件目录
     * @param content  待写入内容
     * @throws IOException
     */
    public static boolean bufferedOutputStreamMethod(String filepath, byte[] content, boolean append) {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                new FileOutputStream(filepath, append))) {
            bufferedOutputStream.write(content);
            return true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean writeObject(String filePath, Object content) {
        ObjectOutputStream objectwriter = null;
        try {
            objectwriter = new ObjectOutputStream(new FileOutputStream(filePath));
            objectwriter.writeObject(content);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                objectwriter.close();
            } catch (IOException e) {
                // TODO自动生成的 catch 块
                e.printStackTrace();
            }
        }
    }


}
