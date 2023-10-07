package com.cbp.thread;

/**
 * @Author changbp
 * @Date 2022-12-22 16:04
 * @Return
 * @Version 1.0
 */
public class ThreadDemo {
    public static void main(String[] args) {
        final MyThread myThread = new MyThread();
        myThread.start();
    }
}

class MyThread extends Thread {
    @Override
    public void run() {
        System.out.println(1);
    }
}
