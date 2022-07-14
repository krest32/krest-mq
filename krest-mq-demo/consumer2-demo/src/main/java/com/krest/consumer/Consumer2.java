package com.krest.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.krest")
public class Consumer2 {
    public static void main(String[] args) {
        SpringApplication.run(Consumer2.class, args);
    }
}
