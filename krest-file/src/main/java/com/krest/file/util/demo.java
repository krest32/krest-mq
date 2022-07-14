package com.krest.file.util;

import java.io.File;

public class demo {
    public static void main(String[] args) {
        int a =  500 * 1024 * 1024;
        System.out.println(a);

        File file = new File("D:\\1.txt");
        System.out.println(file.length());
    }
}
