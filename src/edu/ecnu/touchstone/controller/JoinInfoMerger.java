package edu.ecnu.touchstone.controller;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.Map.Entry;

/**
 * main function: merge the 'pkJoinInfoList'
 *
 * @author liyuming & wangqingshuai
 */
public class JoinInfoMerger {

    /**
     * merge pkJoin without left outer join
     * it will merge all lists and sublist the merge result if the length geq the pkvsMaxSize
     *
     * @param pkJoinInfoList the list of all pkJoinInfo
     * @param pkvsMaxSize    the max size of each status key
     * @return the merged join info
     */
    public static Map<Integer, ArrayList<long[]>> merge(List<Map<Integer,
            ArrayList<long[]>>> pkJoinInfoList, int pkvsMaxSize) {
        Map<Integer, ArrayList<long[]>> mergedPkJoinInfo = pkJoinInfoList.get(0);

        for (int i = 1; i < pkJoinInfoList.size(); i++) {
            Map<Integer, ArrayList<long[]>> pkJoinInfo = pkJoinInfoList.get(i);

            for (Entry<Integer, ArrayList<long[]>> entry : pkJoinInfo.entrySet()) {
                if (!mergedPkJoinInfo.containsKey(entry.getKey())) {
                    mergedPkJoinInfo.put(entry.getKey(), entry.getValue());
                } else {
                    ArrayList<long[]> list = mergedPkJoinInfo.get(entry.getKey());
                    list.addAll(entry.getValue());
                    Collections.shuffle(list);
                    if (list.size() > pkvsMaxSize) {
                        list = new ArrayList<>(list.subList(0, pkvsMaxSize));
                    }
                    mergedPkJoinInfo.put(entry.getKey(), list);
                }
            }
        }
        return mergedPkJoinInfo;
    }

    /**
     * controller merge all pkJoinInfoList in left join mode
     *
     * @param pkJoinInfoList the list of all pkJoinInfo
     * @return the merged pkJoin Info which has deleted the value which is null and
     * the file size for each left outer join status
     */
    static Pair<Map<Integer, ArrayList<long[]>>, Map<Integer, Long[]>> mergeLeftOuterJoin(
            List<Map<Integer, ArrayList<long[]>>> pkJoinInfoList) {
        //merge pk join info and compute the file size for every left join status
        Map<Integer, ArrayList<long[]>> mergedPkJoinInfo = new HashMap<>(16);
        Map<Integer, Long> mergedFileSizeInfo = new HashMap<>(mergedPkJoinInfo.size());

        for (Map<Integer, ArrayList<long[]>> pkJoinInfo : pkJoinInfoList) {
            for (Entry<Integer, ArrayList<long[]>> joinStatusInfo : pkJoinInfo.entrySet()) {
                //get the join key list
                ArrayList<long[]> joinKeyList = joinStatusInfo.getValue();

                //if it has a file size value, pop it
                if (joinKeyList.get(joinKeyList.size() - 1)[0] < 0) {
                    if (!mergedFileSizeInfo.containsKey(joinStatusInfo.getKey())) {
                        mergedFileSizeInfo.put(joinStatusInfo.getKey(),
                                -joinKeyList.remove(joinKeyList.size() - 1)[0]);
                    } else {
                        mergedFileSizeInfo.put(joinStatusInfo.getKey(), mergedFileSizeInfo.get(joinStatusInfo.getKey())
                                - joinKeyList.remove(joinKeyList.size() - 1)[0]);
                    }
                }

                //merge pk join info list
                if (!mergedPkJoinInfo.containsKey(joinStatusInfo.getKey())) {
                    mergedPkJoinInfo.put(joinStatusInfo.getKey(), joinKeyList);
                } else {
                    mergedPkJoinInfo.get(joinStatusInfo.getKey()).addAll(joinKeyList);
                }
            }
        }

        //shuffle pk join info list
        for (ArrayList<long[]> joinInfoList : mergedPkJoinInfo.values()) {
            Collections.shuffle(joinInfoList);
        }

        //compute join table size for every left join status
        Map<Integer, Long[]> mergedSizeInfo = new HashMap<>(mergedPkJoinInfo.size());
        for (Entry<Integer, ArrayList<long[]>> joinStatusInfo : mergedPkJoinInfo.entrySet()) {
            int status = joinStatusInfo.getKey();
            if (!mergedFileSizeInfo.containsKey(status)) {
                mergedSizeInfo.put(status, new Long[]{(long) joinStatusInfo.getValue().size(), 0L});
            } else {
                mergedSizeInfo.put(status, new Long[]{(long) joinStatusInfo.getValue().size(),
                        mergedFileSizeInfo.get(status)});
            }
        }


        return new ImmutablePair<>(mergedPkJoinInfo, mergedSizeInfo);
    }

    /**
     * data generator merge pk join info list and file size for every status
     *
     * @param pkJoinInfoList        the list of all pkJoinInfo
     * @param leftOuterJoinFileSize file size for every left join status
     * @return the merged pk join info
     */
    public static Map<Integer, ArrayList<long[]>> merge(List<Map<Integer, ArrayList<long[]>>> pkJoinInfoList,
                                                        Map<Integer, Long> leftOuterJoinFileSize) {
        Map<Integer, ArrayList<long[]>> mergedPkJoinInfo = new HashMap<>(16);

        //merge pk join info list
        for (Map<Integer, ArrayList<long[]>> pkJoinInfo : pkJoinInfoList) {
            for (Entry<Integer, ArrayList<long[]>> entry : pkJoinInfo.entrySet()) {
                if (!mergedPkJoinInfo.containsKey(entry.getKey())) {
                    mergedPkJoinInfo.put(entry.getKey(), entry.getValue());
                } else {
                    mergedPkJoinInfo.get(entry.getKey()).addAll(entry.getValue());
                }
            }
        }

        //shuffle pk join info list
        for (ArrayList<long[]> joinInfoList : mergedPkJoinInfo.values()) {
            Collections.shuffle(joinInfoList);
        }

        //add file size for every left join status in the end of pk join info list
        for (Integer status : mergedPkJoinInfo.keySet()) {
            mergedPkJoinInfo.get(status).add(new long[]{leftOuterJoinFileSize.get(status)});
        }
        return mergedPkJoinInfo;
    }
}
