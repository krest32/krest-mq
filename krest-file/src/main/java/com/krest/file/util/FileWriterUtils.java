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
        File file = new File(filePath);
        if (!file.exists() && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

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

    public static int deleteDirectory(File file) {

        int delCount = 0;
        // 如果dir对应的文件不存在，或者不是一个目录，则退出
        if (!file.exists() || !file.isDirectory()) {
            return 0;
        }
        // 删除文件夹下的所有文件(包括子目录)
        File[] files = file.listFiles();
        for (int i = 0; i < files.length; i++) {
            // 删除子文件
            if (files[i].isFile()) {
                delCount += deleteFile(files[i]);
            } else {
                // 删除子目录
                delCount += deleteDirectory(files[i]);
            }
        }
        delCount += file.delete() ? 1 : 0;
        // 删除当前目录
        return delCount;
    }

    public static int deleteFile(File file) {
        try {
            // 如果文件不存在直接返回true
            if (!file.exists()) {
                return 0;
            }
            // 路径为文件且不为空则进行删除
            if (file.isFile() && file.exists()) {
                boolean delFlag = false;
                for (int i = 0; i < 10; i++) {
                    delFlag = file.delete();
                    if (delFlag) {
                        return 1;
                    } else {
                        // 如果删除不成功，那就就是用GC，然后进行强制删除，但是这种方法不推荐
                        System.gc();
                        Thread.sleep(1000);
                    }
                }
            }
            return 0;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
