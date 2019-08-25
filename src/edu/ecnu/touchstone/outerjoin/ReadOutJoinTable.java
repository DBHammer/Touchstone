package edu.ecnu.touchstone.outerjoin;

import edu.ecnu.touchstone.run.Touchstone;
import org.apache.log4j.Logger;

import java.io.*;
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
    private static int maxNumOfJoinInfoInReadQueue;
    private static int maxSizeOfJoinInfoInMemory;
    private int currentSize;

    private boolean readCompleted;

    public static void setMaxSizeOfJoinInfoInMemory(int maxSizeOfJoinInfoInMemory) {
        ReadOutJoinTable.maxSizeOfJoinInfoInMemory = maxSizeOfJoinInfoInMemory;
    }

    public static void setMinSizeofJoinStatus(int minSizeofJoinStatus) {
        ReadOutJoinTable.minSizeofJoinStatus = minSizeofJoinStatus;
    }

    public static void setMaxNumOfJoinInfoInReadQueue(int maxNumOfJoinInfoInReadQueue) {
        ReadOutJoinTable.maxNumOfJoinInfoInReadQueue = maxNumOfJoinInfoInReadQueue;
    }

    private Map<Integer, Double> statusNullProbability;
    private int leftOuterTag;


    public ReadOutJoinTable(String joinTableReadPath, Map<Integer, Double> statusNullProbability, int leftOuterTag) {
        this.joinTableReadPath = joinTableReadPath;
        this.statusNullProbability = statusNullProbability;
        this.leftOuterTag = leftOuterTag;
        joinInfoQueue = new LinkedBlockingQueue<>(maxNumOfJoinInfoInReadQueue);
        joinInfoInMemory = readOutJoinTable();
        for (List<long[]> value : joinInfoInMemory.values()) {
            currentSize += value.size();
        }
    }

    private Map<Integer, List<long[]>> readOutJoinTable() {
        Map<Integer, List<long[]>> readJoinInfo = new HashMap<>();
        try (ObjectInputStream joinTableOutputStream = new ObjectInputStream(new FileInputStream(
                new File(joinTableReadPath + (++readIndex))))) {
            //If the read object is not a instance of joinTableInfo,
            // it maybe cause an error. But in fact it must not be.
            readJoinInfo = (Map<Integer, List<long[]>>) joinTableOutputStream.readObject();
            for (Map.Entry<Integer, List<long[]>> joinInfo : readJoinInfo.entrySet()) {
                int clearIndex=(int)Math.round(statusNullProbability.get(joinInfo.getKey() & leftOuterTag)
                        * joinInfo.getValue().size());
                if(clearIndex==joinInfo.getValue().size()){
                    clearIndex--;
                }
                joinInfo.getValue().subList(0, clearIndex).clear();
            }
            logger.debug("read join info complete, the file is" + joinTableReadPath + readIndex);
            return readJoinInfo;
        } catch (FileNotFoundException e) {
            logger.debug("file read end or file path error");
            return new HashMap<>(16);
        } catch (IOException | ClassNotFoundException e) {
            logger.error(e);
            logger.error("error path is" + joinTableReadPath + readIndex);
            return new HashMap<>(16);
        } catch (Exception e){
            System.out.println(leftOuterTag);
            System.out.println("key1");
            for (Integer key : readJoinInfo.keySet()) {
                System.out.println(key);
            }
            System.out.println("key2");
            for (Integer key : statusNullProbability.keySet()) {
                System.out.println(key);
            }
            logger.error(e);
            return new HashMap<>(16);
        }
    }

    /**
     * 根据status获取一个非重复的key，当该status获取完毕时，返回null
     */
    public long[] getJoinKey(Integer status) {
        if (currentSize == 0) {
            return null;
        }
        List<long[]> keys = null;
        if (joinInfoInMemory.containsKey(status)) {
            keys = joinInfoInMemory.get(status);
        } else {
            //找到值最多的list
            for (Map.Entry<Integer, List<long[]>> joinInfo : joinInfoInMemory.entrySet()) {
                if ((status & joinInfo.getKey()) == joinInfo.getKey()) {
                    if (keys == null || joinInfo.getValue().size() >= keys.size()) {
                        keys = joinInfo.getValue();
                    }
                }
            }
            //没有找到则返回null
            if (keys == null) {
                return null;
            }
        }

        //找到了，但是已经读取完毕也返回null
        if (readCompleted && keys.size() == 0) {
            return null;
        }

        //如果内存中joinTable值不足，且文件没有读取完毕，且内存的join table小于界定值，则拉取新的joinInfo
        if (keys.size() < minSizeofJoinStatus && !readCompleted && currentSize < maxSizeOfJoinInfoInMemory) {
            try {
                Map<Integer, List<long[]>> readJoinInfo = joinInfoQueue.take();
                if (readJoinInfo.size() == 0) {
                    readCompleted = true;
                }else {
                    //merge read joinInfo into joinInfoInMemory
                    for (Map.Entry<Integer, List<long[]>> joinInfo : readJoinInfo.entrySet()) {
                        currentSize += joinInfo.getValue().size();
                        if (joinInfoInMemory.containsKey(joinInfo.getKey())) {
                            joinInfoInMemory.get(joinInfo.getKey()).addAll(joinInfo.getValue());
                        } else {
                            joinInfoInMemory.put(joinInfo.getKey(), joinInfo.getValue());
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if(keys.size()>0){
            currentSize--;
            return keys.remove(0);
        }else {
            return null;
        }
    }

    /**
     * 停止该线程
     */
    public void stopThread() {
        joinInfoInMemory = null;
        stop = true;
        joinInfoQueue.clear();
    }

    @Override
    public void run() {
        while (true) {
            Map<Integer, List<long[]>> joinInfo = readOutJoinTable();
            try {
                joinInfoQueue.put(joinInfo);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (stop || joinInfo.size() == 0) {
                return;
            }
        }
    }
}
