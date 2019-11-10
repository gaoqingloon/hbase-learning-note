package com.lolo.bigdata.test;

public class TestThread {
    public static void main(String[] args) throws Exception {

        //new TestThread().wait();
        // Exception in thread "main" java.lang.IllegalMonitorStateException
        // 不是锁，是监听器Monitor

        // sleep(不释放对象锁)
        /*TestThread testThread = new TestThread();
        synchronized (testThread) {
            testThread.wait();
        }*/

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            sb.append(i);
        }
        System.out.println(sb);
    }
}
