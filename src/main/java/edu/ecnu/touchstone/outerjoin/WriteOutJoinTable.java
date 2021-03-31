package edu.ecnu.touchstone.outerjoin;

import edu.ecnu.touchstone.run.Touchstone;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author wangqingshuai
 */
public class WriteOutJoinTable implements Runnable {

    /**
     * 缓冲队列中最大的join info表的数量
     */
    private static int maxNumOfJoinInfoInWriteQueue;
    /**
     * JoinInfo最大承载的Size
     */
    private static int maxSizeofJoinInfoInMemory;
    private Logger logger = Logger.getLogger(Touchstone.class);
    private String joinTableWritePath;
    private int writeIndex;
    private BlockingQueue<Map<Integer, List<long[]>>> joinInfoQueue;
    private Map<Integer, List<long[]>> joinInfoInMemory;
    /**
     * 当前JoinInfo中的Size
     */
    private int currentSizeOfJoinInfoInMemory;

    public WriteOutJoinTable(String joinTableWritePath) {
        this.joinTableWritePath = joinTableWritePath;
        joinInfoQueue = new LinkedBlockingQueue<>(maxNumOfJoinInfoInWriteQueue);
        joinInfoInMemory = new HashMap<>();
    }

    public static void setMaxSizeofJoinInfoInMemory(int maxSizeofJoinInfoInMemory) {
        WriteOutJoinTable.maxSizeofJoinInfoInMemory = maxSizeofJoinInfoInMemory;
    }

    public static void setMaxNumOfJoinInfoInWriteQueue(int maxNumOfJoinInfoInWriteQueue) {
        WriteOutJoinTable.maxNumOfJoinInfoInWriteQueue = maxNumOfJoinInfoInWriteQueue;
    }

    public void write(Integer status, long[] key) {
        if (joinInfoInMemory.containsKey(status)) {
            joinInfoInMemory.get(status).add(key);
        } else {
            ArrayList<long[]> keys = new ArrayList<>();
            keys.add(key);
            joinInfoInMemory.put(status, keys);
        }
        if (++currentSizeOfJoinInfoInMemory > maxSizeofJoinInfoInMemory) {
            try {
                joinInfoQueue.put(joinInfoInMemory);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            joinInfoInMemory = new HashMap<>(16);
            currentSizeOfJoinInfoInMemory = 0;
        }

    }

    /**
     * 停止该线程
     */
    public void stopThread() {
        try {
            //将剩余的size写出
            joinInfoQueue.put(joinInfoInMemory);
            //停止线程的标志
            joinInfoQueue.put(new HashMap<>(16));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        while (true) {
            Map<Integer, List<long[]>> joinInfo = null;
            try {
                joinInfo = joinInfoQueue.take();
            } catch (InterruptedException e) {
                logger.error(e);
            }
            //end tag
            if (joinInfo == null || joinInfo.size() == 0) {
                return;
            }
            try (ObjectOutputStream joinTableOutputStream = new ObjectOutputStream(new FileOutputStream(
                    new File(joinTableWritePath + (++writeIndex))))) {
                joinTableOutputStream.writeObject(joinInfo);
            } catch (IOException e) {
                logger.error(e);
            }
        }
    }
}
