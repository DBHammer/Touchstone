package edu.ecnu.touchstone.outerjoin;

import edu.ecnu.touchstone.run.Touchstone;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author wangqingshuai
 * run a thread to read file in order.
 * if the joinInfoInMemory is less than the config value, it will continue to read.
 */
public class ReadOutJoinTable implements Runnable {

    private boolean stop;
    private Logger logger = Logger.getLogger(Touchstone.class);

    private String joinTableReadPath;
    private int readIndex;

    private Map<Integer, List<long[]>> joinInfoInMemory;
    private BlockingQueue<Map<Integer, List<long[]>>> joinInfoQueue;


    private static int minSizeInMemory;
    private static int maxSizeInMemory;
    private static int maxJoinInfoSize;

    private boolean readCompleted;
    private boolean asynchronousMerging;

    public static void setMinSizeInMemory(int minSizeInMemory) {
        ReadOutJoinTable.minSizeInMemory = minSizeInMemory;
    }

    public static void setMaxSizeInMemory(int maxSizeInMemory) {
        ReadOutJoinTable.maxSizeInMemory = maxSizeInMemory;
    }

    public static void setMaxJoinInfoSize(int maxJoinInfoSize) {
        ReadOutJoinTable.maxJoinInfoSize = maxJoinInfoSize;
    }

    private Map<Integer, Long> statusMustExistSize;
    private Map<Integer, Long> statusNullSize;


    public ReadOutJoinTable(String joinTableReadPath, Map<Integer, Long> statusMustExistSize,
                            Map<Integer, Long> statusNullSize) {
        this.joinTableReadPath = joinTableReadPath;
        this.statusMustExistSize = statusMustExistSize;
        this.statusNullSize = statusNullSize;
        joinInfoQueue = new LinkedBlockingQueue<>(maxJoinInfoSize);
        joinInfoInMemory = readOutJoinTable();
    }

    private Map<Integer, List<long[]>> readOutJoinTable() {
        try (ObjectInputStream joinTableOutputStream = new ObjectInputStream(new FileInputStream(
                new File(joinTableReadPath + (++readIndex))))) {
            //If the read object is not a instance of joinTableInfo,
            // it maybe cause an error. But in fact it must not be.
            return (Map<Integer, List<long[]>>) joinTableOutputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.error(e);
            return new HashMap<>();
        }
    }

    private void takeAndMerge() throws InterruptedException {
        Map<Integer, List<long[]>> readJoinInfo = joinInfoQueue.take();
        if (readJoinInfo.size() == 0) {
            readCompleted = true;
            asynchronousMerging = false;
            return;
        }

        //merge read joinInfo into joinInfoInMemory
        for (Map.Entry<Integer, List<long[]>> joinInfo : readJoinInfo.entrySet()) {
            if (joinInfoInMemory.containsKey(joinInfo.getKey())) {
                List<long[]> pks = joinInfoInMemory.get(joinInfo.getKey());
                //如果该status已经生成完毕，则不join新的value
                if (statusMustExistSize.get(joinInfo.getKey()) > pks.size()) {
                    pks.addAll(joinInfo.getValue());
                    //如果null size=0，或者内存中的size不足，则不允许删除
                    long nullSize = statusNullSize.get(joinInfo.getKey());
                    if (nullSize > 0 && pks.size() > maxSizeInMemory) {
                        Collections.shuffle(pks);
                        long subSize = pks.size() - maxSizeInMemory;
                        //不允许多删
                        if (subSize > nullSize) {
                            pks.subList(0, (int) (pks.size() - nullSize)).clear();
                            statusNullSize.put(joinInfo.getKey(), 0L);
                        } else {
                            statusNullSize.put(joinInfo.getKey(), nullSize - subSize);
                            pks.subList(0, maxSizeInMemory).clear();
                        }
                    }
                }
            } else {
                //如果该status已经生成完毕，则不join新的value
                if (statusMustExistSize.get(joinInfo.getKey()) > 0) {
                    joinInfoInMemory.put(joinInfo.getKey(), joinInfo.getValue());
                }
            }
        }
        asynchronousMerging = false;
    }

    /**
     * 根据status获取一个非重复的key，当该status获取完毕时，返回null
     */
    public long[] getJoinKey(Integer status) {
        List<long[]> keys = new ArrayList<>();
        if (statusMustExistSize.containsKey(status)) {
            if (statusMustExistSize.get(status) == 0) {
                return null;
            }
            statusMustExistSize.put(status, statusMustExistSize.get(status) - 1);
            keys = joinInfoInMemory.get(status);
        } else {
            //找到值最多的list
            for (Map.Entry<Integer, List<long[]>> joinInfo : joinInfoInMemory.entrySet()) {
                if ((status & joinInfo.getKey()) == joinInfo.getKey() && joinInfo.getValue().size() >= keys.size()) {
                    keys = joinInfo.getValue();
                }
            }
        }
        //如果内存中joinTable值不足，则拉取新的joinInfo
        if (!readCompleted && !asynchronousMerging && keys.size() < minSizeInMemory) {
            asynchronousMerging = true;
            new Thread(() -> {
                try {
                    takeAndMerge();
                } catch (InterruptedException e) {
                    logger.error(e);
                }
            }).start();
        }
        return keys.remove(0);
    }

    /**
     * 停止该线程
     */
    public void stopThread() {
        joinInfoQueue.clear();
        stop = true;
    }

    @Override
    public void run() {
        while (true) {
            Map<Integer, List<long[]>> joinInfo = readOutJoinTable();
            joinInfoQueue.add(joinInfo);
            if (stop || joinInfo.size() == 0) {
                return;
            }
        }
    }
}
