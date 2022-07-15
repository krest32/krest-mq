package com.krest.file.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileReaderUtils {

    public static List<String> readFile(String filePath) {
        List<String> ans = new ArrayList<>();
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(new File(filePath), "UTF-8");
            while (it.hasNext()) {
                ans.add(it.nextLine());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != it)
                    it.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ans;
    }


    public static String loadFileAsString(String fileName) {
        if (fileName == null || fileName.length() == 0) {
            throw new IllegalArgumentException("Operate File's Name Argument Exception.");
        } else {
            File operFile = new File(fileName);
            String line = "";
            StringBuilder fileResult = new StringBuilder();
            try (InputStreamReader isr =
                         new InputStreamReader(new FileInputStream(operFile), "UTF-8");
                 BufferedReader reader = new BufferedReader(isr);) {
                while ((line = reader.readLine()) != null) {
                    if (!line.equals("")) {
                        fileResult.append(line.trim());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return fileResult.toString();
        }
    }


    public static Object readObject(String filePath) {
        ObjectInputStream objectreader = null;
        try {
            objectreader = new ObjectInputStream(new FileInputStream(filePath));
            return objectreader.readObject();
        } catch (IOException | ClassNotFoundException e) {
            // TODO自动生成的 catch 块
            e.printStackTrace();
            return null;
        } finally {
            try {
                objectreader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
