package edu.ecnu.touchstone.outerjoin;

import edu.ecnu.touchstone.run.Touchstone;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /**
     * status是全表的status 未被压缩过
     */
    private Map<Integer, List<long[]>> joinInfoInMemory;
    private BlockingQueue<Map<Integer, List<long[]>>> joinInfoQueue;


    private static int minSizeofJoinStatus;
    private static int maxNumOfJoinInfoInMemory;

    private boolean readCompleted;
    private boolean asynchronousMerging;

    public static void setMinSizeofJoinStatus(int minSizeofJoinStatus) {
        ReadOutJoinTable.minSizeofJoinStatus = minSizeofJoinStatus;
    }

    public static void setMaxNumOfJoinInfoInMemory(int maxNumOfJoinInfoInMemory) {
        ReadOutJoinTable.maxNumOfJoinInfoInMemory = maxNumOfJoinInfoInMemory;
    }

    private Map<Integer, double[]> statusNullProbability;
    private int leftOuterTag;


    public ReadOutJoinTable(String joinTableReadPath, Map<Integer, double[]> statusNullProbability, int leftOuterTag) {
        this.joinTableReadPath = joinTableReadPath;
        this.statusNullProbability = statusNullProbability;
        this.leftOuterTag = leftOuterTag;
        joinInfoQueue = new LinkedBlockingQueue<>(maxNumOfJoinInfoInMemory);
        joinInfoInMemory = readOutJoinTable();
    }

    private Map<Integer, List<long[]>> readOutJoinTable() {
        try (ObjectInputStream joinTableOutputStream = new ObjectInputStream(new FileInputStream(
                new File(joinTableReadPath + (++readIndex))))) {
            //If the read object is not a instance of joinTableInfo,
            // it maybe cause an error. But in fact it must not be.
            Map<Integer, List<long[]>> readJoinInfo = (Map<Integer, List<long[]>>) joinTableOutputStream.readObject();
            for (Map.Entry<Integer, List<long[]>> joinInfo : readJoinInfo.entrySet()) {
                joinInfo.getValue().subList(0, (int) (statusNullProbability.get(joinInfo.getKey() & leftOuterTag)[1]
                        * joinInfo.getValue().size())).clear();
            }
            return readJoinInfo;
        } catch (IOException | ClassNotFoundException e) {
            logger.error(e);
            return new HashMap<>(16);
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
                joinInfoInMemory.get(joinInfo.getKey()).addAll(joinInfo.getValue());
            } else {
                joinInfoInMemory.put(joinInfo.getKey(), joinInfo.getValue());
            }
        }
        asynchronousMerging = false;
    }

    /**
     * 根据status获取一个非重复的key，当该status获取完毕时，返回null
     */
    public long[] getJoinKey(Integer status) {
        List<long[]> keys = new ArrayList<>();

        if (joinInfoInMemory.containsKey(status)) {
            keys = joinInfoInMemory.get(status);
        } else {
            //找到值最多的list
            for (Map.Entry<Integer, List<long[]>> joinInfo : joinInfoInMemory.entrySet()) {
                if ((status & joinInfo.getKey()) == joinInfo.getKey() && joinInfo.getValue().size() >= keys.size()) {
                    keys = joinInfo.getValue();
                }
            }
        }
        long[] key = keys.remove(0);

        //如果内存中joinTable值不足，则异步拉取新的joinInfo
        if (!readCompleted && !asynchronousMerging && keys.size() < minSizeofJoinStatus) {
            asynchronousMerging = true;
            new Thread(() -> {
                try {
                    takeAndMerge();
                } catch (InterruptedException e) {
                    logger.error(e);
                }
            }).start();
        }

        return key;
    }

    /**
     * 停止该线程
     */
    public void stopThread() {
        joinInfoInMemory = null;
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
