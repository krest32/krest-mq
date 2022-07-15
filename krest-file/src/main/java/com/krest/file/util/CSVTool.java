package com.krest.file.util;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CSVTool {

    private CSVTool() {
    }

    /**
     * 返回整行内容
     */
    public static List<String> getText(String csvPath) {
        List<String> resList = new ArrayList<String>();
        // 第一参数：读取文件的路径 第二个参数：分隔符 第三个参数：字符集
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);
            // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
            csvReader.readHeaders();
            // 读取每行的内容
            while (csvReader.readRecord()) {
                resList.add(csvReader.getRawRecord());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            csvReader.close();
        }

        return resList;
    }

    /**
     * 读取第几列中的内容
     */
    public static List<String> read(String csvPath, Integer columnNumber) throws IOException {
        List<String> resList = new ArrayList<String>();
        // 第一参数：读取文件的路径 第二个参数：分隔符 第三个参数：字符集
        CsvReader csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);
        // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
        csvReader.readHeaders();
        // 读取每行的内容
        while (csvReader.readRecord()) {
            // 将数据加入到List中
            resList.add(csvReader.get(columnNumber));
        }
        return resList;
    }


    /**
     * 获取第x行内容
     */
    public static String getRowText(String csvPath, boolean ignoreHeader, int row) throws IOException {
        CsvReader csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);
        if (ignoreHeader) {
            // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
            csvReader.readHeaders();
        }
        int i = 0;
        String s = "";
        // 读取每行的内容
        while (csvReader.readRecord()) {
            i++;
            if (i == row) {
                s = csvReader.getRawRecord();
                break;
            }
        }
        return s;
    }

    /**
     * 获取第x行x列内容
     */
    public static String getRowColumnText(boolean ignoreHeader, String csvPath, int row, int column) throws IOException {
        CsvReader csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);
        if (ignoreHeader) {
            // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
            csvReader.readHeaders();
        }
        int i = 0;
        String s = "";
        // 读取每行的内容
        while (csvReader.readRecord()) {
            i++;
            if (i == row) {
                s = csvReader.get(column - 1);
            }
        }
        return s;
    }


    /**
     * 批量获取某一列的信息
     */
    public static List<String> getListColumn(boolean ignoreHeader, String csvPath, int column) {
        List<String> ans = new ArrayList<>();
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);

            if (ignoreHeader) {
                // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
                csvReader.readHeaders();
            }
            int i = 0;
            String s = "";
            // 读取每行的内容
            while (csvReader.readRecord()) {
                i++;
                s = csvReader.get(column - 1 < 0 ? 0 : column - 1);
                ans.add(s);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ans;
    }

    /**
     * 获取第x行x列内容,根据表头确定列数
     */
    public static String getRowColumnText1(boolean ignoreHeader, String csvPath, int row, String column) throws IOException {
        CsvReader csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);
        if (ignoreHeader) {
            // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
            csvReader.readHeaders();
        }
        int i = 0;
        String s = "";
        // 读取每行的内容
        while (csvReader.readRecord()) {
            i++;
            if (i == row) {
                s = csvReader.get(column);
                break;
            }
        }
        return s;
    }

    /**
     * 获取分隔符
     */
    public static char getSeparator(String csvPath) throws IOException {
        CsvReader csvReader = new CsvReader(csvPath);
        csvReader.close();
        return csvReader.getDelimiter();
    }

    /**
     * 文件内容的总行数。
     */
    public static int getRows(String csvPath, boolean ignoreHeader) throws IOException {
        // 第一参数：读取文件的路径 第二个参数：分隔符 第三个参数：字符集
        CsvReader csvReader = new CsvReader(csvPath, ',', StandardCharsets.UTF_8);
        if (ignoreHeader) {
            // 如果文件没有表头，这行不用执行.从表头的下一行读，也就是过滤表头
            csvReader.readHeaders();
        }
        int i = 0;
        // 读取每行的内容
        while (csvReader.readRecord()) {
            i++;
        }
        return i;
    }

    /**
     * 写csv文件
     */
    public static void write(String path, String[] header, List<String[]> content) {
        // 创建CSV写对象
        CsvWriter csvWriter = new CsvWriter(path, ',', StandardCharsets.UTF_8);
        // 写表头
        try {
            csvWriter.writeRecord(header);
            for (String[] list : content) {
                csvWriter.writeRecord(list);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            csvWriter.close();
        }
    }

    //获取表头
    public static String[] getHeader(String path) throws IOException {
        CsvReader csvReader = new CsvReader(path, ',', StandardCharsets.UTF_8);
        csvReader.readHeaders();
        return csvReader.getHeaders();
    }
}
