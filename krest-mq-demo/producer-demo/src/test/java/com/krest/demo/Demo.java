package com.krest.demo;

public class Demo {
    public static void main(String[] args) {
        TestInterface testInterface =TestFactory.create(TestInterface.class).build();
        testInterface.testMethod1();
        testInterface.testMethod2();
    }
}
