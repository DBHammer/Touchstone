package edu.ecnu.touchstone.threadpool;

/**
 * @author wangqingshuai
 * 线程工厂，暂时没有线程检测等其他功能
 */
public class TouchStoneThreadFactory implements java.util.concurrent.ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r);
    }
}
