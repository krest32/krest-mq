package com.krest.file.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class DemoBlockQueue {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        LinkedBlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue<>(2);
        LinkedBlockingDeque<Integer> deque = new LinkedBlockingDeque<>();
        deque.putFirst(1);
        deque.putFirst(2);
        System.out.println(deque.takeLast());
        deque.addLast(3);
        System.out.println(deque.takeFirst());
    }
}
