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

public class WriteOutJoinTable implements Runnable {

    private Logger logger = Logger.getLogger(Touchstone.class);

    private String joinTableWritePath;
    private int writeIndex;

    private BlockingQueue<Map<Integer, List<long[]>>> joinInfoQueue;
    private Map<Integer, List<long[]>> joinInfoInMemory;

    /**
     * 缓冲队列中最大的join info表的数量
     */
    private static int maxNumInMemory;

    /**
     * 当前JoinInfo中的Size
     */
    private int currentJoinInfoSizeInMemory;

    /**
     * JoinInfo最大承载的Size
     */
    private static int maxSizeofJoinInfoInMemory;

    public static void setMaxSizeofJoinInfoInMemory(int maxSizeofJoinInfoInMemory) {
        WriteOutJoinTable.maxSizeofJoinInfoInMemory = maxSizeofJoinInfoInMemory;
    }

    public static void setMaxNumInMemory(int maxNumInMemory) {
        WriteOutJoinTable.maxNumInMemory = maxNumInMemory;
    }

    public WriteOutJoinTable(String joinTableWritePath) {
        this.joinTableWritePath = joinTableWritePath;
        joinInfoQueue = new LinkedBlockingQueue<>(maxNumInMemory);
        joinInfoInMemory = new HashMap<>();
    }

    public void write(Integer status, long[] key) {
        if (joinInfoInMemory.containsKey(status)) {
            joinInfoInMemory.get(status).add(key);
        } else {
            ArrayList<long[]> keys = new ArrayList<>();
            keys.add(key);
            joinInfoInMemory.put(status, keys);
        }
        if (++currentJoinInfoSizeInMemory > maxSizeofJoinInfoInMemory) {
            joinInfoQueue.add(joinInfoInMemory);
            joinInfoInMemory = new HashMap<>();
        }

    }

    /**
     * 停止该线程
     */
    public void stopThread() {
        //将剩余的size写出
        joinInfoQueue.add(joinInfoInMemory);
        //停止线程的标志
        joinInfoQueue.add(new HashMap<>());
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
