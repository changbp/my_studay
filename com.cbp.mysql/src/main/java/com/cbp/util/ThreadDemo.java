package com.cbp.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.*;
import java.util.stream.Collectors;

/**
 * @Author changbp
 * @Date 2021-06-07 9:53
 * @Return
 * @Version 1.0
 */
public class ThreadDemo {
    public static void main(String[] args) throws InterruptedException {
//        System.out.println("开始");
//        final Thread myThread = new MyThread();
//        myThread.start();
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        final Thread thread = new Thread(new MyRunable());
//        thread.start();
//        System.out.println("结束");
//        System.out.println("main start...");
//        Thread t = new Thread(() -> {
//            System.out.println("thread run...");
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {}
//            System.out.println("thread end.");
//        });
//        t.start();
//        try {
//            Thread.sleep(10);
//        } catch (InterruptedException e) {}
//        System.out.println("main end...");

//        Thread t = new Thread(() -> {
//            System.out.println("hello");
//        });
//        System.out.println("start");
//        t.start();
//        Thread.sleep(100);
//        t.interrupt();
//        t.join();
//        System.out.println("end");
        Thread t = new MyThread();
        t.start();
        Thread.sleep(1000);
        t.interrupt(); // 中断t线程
        t.join(); // 等待t线程结束
        System.out.println("end");
        final Lock reentrantLock = new ReentrantLock();
        final ReadWriteLock lock = new ReentrantReadWriteLock();
        final Lock writeLock = lock.writeLock();
        final Lock readLock = lock.readLock();
        final StampedLock stampedLock = new StampedLock();
        final long l = stampedLock.tryOptimisticRead();
        ExecutorService executorService = Executors.newSingleThreadExecutor();


    }

}

class MyThread1 extends Thread {
    @Override
    public void run() {
        int n = 0;
        while (!isInterrupted()) {
            n++;
            System.out.println(n + "Thread 线程开始！！！");
        }
    }
}

class MyRunable implements Runnable {
    @Override
    public void run() {
        System.out.println("Runnable 线程开始！！！");
    }
}

class MyThread extends Thread {
    public void run() {
        Thread hello = new HelloThread();
        hello.start(); // 启动hello线程
        try {
            hello.join(); // 等待hello线程结束
        } catch (InterruptedException e) {
            System.out.println("interrupted!");
        }
        hello.interrupt();
    }
}

class HelloThread extends Thread {
    public void run() {
        int n = 0;
        while (!isInterrupted()) {
            n++;
            System.out.println(n + " hello!");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}