package com.krest.file.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DemoBlockQueue {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BlockingQueue<Integer> blockingQueue = new LinkedBlockingQueue<>(2);
        blockingQueue.put(1);
        blockingQueue.put(2);
        blockingQueue.put(3);
    }
}
