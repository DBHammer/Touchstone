package edu.ecnu.touchstone.pretreatment;

import edu.ecnu.touchstone.constraintchain.*;
import edu.ecnu.touchstone.outerjoin.ReadOutJoinTable;
import edu.ecnu.touchstone.outerjoin.WriteOutJoinTable;
import edu.ecnu.touchstone.queryinstantiation.Parameter;
import edu.ecnu.touchstone.run.Statistic;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.schema.Attribute;
import edu.ecnu.touchstone.threadpool.TouchStoneThreadPool;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

// the generation template of the table
// each thread has such an object
// at one node, all threads can share 'fksJoinInfo' (reduce the memory consumption)
public class TableGeneTemplate implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName;
    private long tableSize;

    // the string representation of the primary key
    private String pkStr = null;

    // there are two types of the key attribute
    // one is generated sequentially, and another is generated according to its join statuses
    // there must be a key generated sequentially (ensure the uniqueness of the primary key)
    private List<Key> keys = null;

    // all non-key attributes
    private List<Attribute> attributes = null;

    // all cardinality constraint chains of this table
    private List<ConstraintChain> constraintChains = null;

    // for getting the join information of referenced primary keys
    private List<String> referencedKeys = null;

    // map: referenced primary key -> corresponding foreign key of this table
    // for setting the value of foreign keys
    private Map<String, String> referKeyForeKeyMap = null;

    // for initializing the 'parsii' in basic filter operations
    private Map<Integer, Parameter> parameterMap = null;
    private transient Map<String, Attribute> attributeMap = null;

    // the maximum number of shuffling the constraint chains for 'adjustFksGeneStrategy'
    private int shuffleMaxNum;

    // the maximum size of pkvs list (the value of map 'pkJoinInfo', for compression algorithm)
    private int pkvsMaxSize;
    // map: Key: the string representation of referenced primary key (support mixed reference)
    //      Value: Key: combined join statuses
    //             Value: a list of primary keys that satisfy the combined join statuses
    private Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo = null;
    // map: string representation of referenced primary key -> the pair of combined join statuses and
    //      the size of corresponding primary keys list
    private transient Map<String, ArrayList<JoinStatusesSizePair>> fksJoinInfoSizeMap = null;
    // satisfied combined join statuses (a & b == b) and the accumulative (*) size of corresponding primary keys list
    private transient List<JoinStatusesSizePair> satisfiedFkJoinInfo = null;
    // map: combined join statuses -> a list of primary keys (support mixed reference)
    // to maintain the join information of the primary key (there is only one primary key)
    // each thread maintains its own information to avoid the conflict
    private transient Map<Integer, ArrayList<long[]>> pkJoinInfo = null;
    // support the compression of 'pkJoinInfo'
    // map: combined join statuses -> so far, the number of primary keys that can satisfy the combined join statuses
    private transient Map<Integer, Long> pkJoinInfoSizeMap = null;
    // to avoid a mass of string manipulations during the tuple generation
    // attrNames of primary key (only one primary key)
    private transient String[] pkStrArr = null;
    // map: string representation of referenced primary key -> attrNames of referenced primary key
    private transient Map<String, String[]> rpkStrToArray = null;
    // the following two maps will be used to generate each tuple, so they are
    // defined as class attributes to improve efficiency
    // map: attrName -> generated value
    private transient Map<String, String> attributeValueMap = null;
    // map: referenced primary key -> combined join statuses
    private transient Map<String, Integer> fkJoinStatusesMap = null;
    private transient Logger logger = null;
    private transient SimpleDateFormat dateSdf = null;
    private transient SimpleDateFormat dateTimeSdf = null;
    /**
     * the next values are for outer join
     */

    //tha table has left join or not
    private int leftOuterJoinTag;
    //the left join null probability for every constrain links
    private Map<Integer, Double> constrainNullProbability;
    //the file link for write object
    private WriteOutJoinTable writeOutJoinTable = null;
    //record the status size in file
    private Map<Integer, Long> pkJoinInfoFileSize = null;
    //the can join sum for every fk join
    private Map<String, Integer> fkJoinStatus;
    //fk left join null probability in file for each fk and each join status
    private Map<String, Map<Integer, Double>> fkLeftJoinInFileNullProbability;
    //fk Join tag
    private Map<String, Integer> fkLeftJoinTag;
    //record the steplength for unique value in join value
    private int fkStepLength;
    //fk index for the thread,tag the fk location has produced in once
    private Map<String, Map<Integer, Integer>> fkLeftJoinInfoExistIndex;
    // map: referenced primary key -> combined outer join statuses
    private transient Map<String, Integer> fkOutJoinStatusesMap = null;
    private Map<String, ReadOutJoinTable> fkReadOutJoinTables;

    public TableGeneTemplate() {
        tableName = "exitProcess";
    }

    public TableGeneTemplate(String tableName, long tableSize, String pkStr, List<Key> keys, List<Attribute> attributes,
                             List<ConstraintChain> constraintChains, List<String> referencedKeys, Map<String, String> referKeyForeKeyMap,
                             Map<Integer, Parameter> parameterMap, Map<String, Attribute> attributeMap, int shuffleMaxNum,
                             int pkvsMaxSize, Map<Integer, Double> constrainNullProbability, Map<String, Integer> fkJoinStatus) {
        super();
        this.tableName = tableName;
        this.tableSize = tableSize;
        this.pkStr = pkStr;
        this.keys = keys;
        this.attributes = attributes;
        this.constraintChains = constraintChains;
        this.referencedKeys = referencedKeys;
        this.referKeyForeKeyMap = referKeyForeKeyMap;
        this.parameterMap = parameterMap;
        this.attributeMap = attributeMap;
        this.shuffleMaxNum = shuffleMaxNum;
        this.pkvsMaxSize = pkvsMaxSize;
        this.fkJoinStatus = fkJoinStatus;
        this.constrainNullProbability = constrainNullProbability;
        for (Integer status : constrainNullProbability.keySet()) {
            leftOuterJoinTag += status;
        }
    }

    public TableGeneTemplate(TableGeneTemplate template) {
        super();
        this.tableName = template.tableName;
        this.tableSize = template.tableSize;
        this.pkStr = template.pkStr;
        this.keys = new ArrayList<>();
        for (int i = 0; i < template.keys.size(); i++) {
            this.keys.add(new Key(template.keys.get(i)));
        }
        this.attributes = new ArrayList<>();
        for (int i = 0; i < template.attributes.size(); i++) {
            this.attributes.add(new Attribute(template.attributes.get(i)));
        }
        this.constraintChains = new ArrayList<>();
        for (int i = 0; i < template.constraintChains.size(); i++) {
            this.constraintChains.add(new ConstraintChain(template.constraintChains.get(i)));
        }
        this.referencedKeys = new ArrayList<>();
        this.referencedKeys.addAll(template.referencedKeys);
        this.referKeyForeKeyMap = new HashMap<>();
        this.referKeyForeKeyMap.putAll(template.referKeyForeKeyMap);
        this.parameterMap = new HashMap<>();
        for (Entry<Integer, Parameter> entry : template.parameterMap.entrySet()) {
            this.parameterMap.put(entry.getKey(), new Parameter(entry.getValue()));
        }
        this.attributeMap = new HashMap<>();
        for (Attribute attribute : this.attributes) {
            this.attributeMap.put(attribute.getAttrName(), attribute);
        }
        this.shuffleMaxNum = template.shuffleMaxNum;
        this.pkvsMaxSize = template.pkvsMaxSize;
        this.leftOuterJoinTag = template.leftOuterJoinTag;
        this.constrainNullProbability = template.constrainNullProbability;
        this.fkJoinStatus = template.fkJoinStatus;
        this.fkLeftJoinInFileNullProbability = template.fkLeftJoinInFileNullProbability;
        this.fkLeftJoinTag = template.fkLeftJoinTag;
        // shallow copy
        this.fksJoinInfo = template.fksJoinInfo;
        init();

        //no copy
        this.fkReadOutJoinTables = null;
        this.writeOutJoinTable = null;
        this.pkJoinInfoFileSize = null;
        this.fkStepLength = 0;
        this.fkLeftJoinInfoExistIndex = null;
        this.fkOutJoinStatusesMap = new HashMap<>();
    }

    // it's set by Controller according to 'referencedKeys'
    public void setFksJoinInfo(Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo) {
        this.fksJoinInfo = fksJoinInfo;
    }

    public void printTraceNumCount() {
        StringBuilder result = new StringBuilder(tableName);
        for (Entry<String, Map<Integer, Integer>> stringMapEntry : fkLeftJoinInfoExistIndex.entrySet()) {
            result.append(";").append(stringMapEntry.getKey());
            StringBuilder resultForTable = new StringBuilder();
            for (Entry<Integer, Integer> integerIntegerEntry : stringMapEntry.getValue().entrySet()) {
                if (integerIntegerEntry.getValue() > 0) {
                    resultForTable.append(',').append(integerIntegerEntry.getKey()).append('=').append(integerIntegerEntry.getValue());
                }
            }
            if (resultForTable.length() == 0) {
                result.append(",read completed");
            } else {
                result.append(resultForTable);
            }
        }
        logger.info(result);
    }

    public void setFkLeftJoinTag(Map<String, Integer> fkLeftJoinTag) {
        this.fkLeftJoinTag = fkLeftJoinTag;
    }

    public void setFkLeftJoinInFileNullProbability(Map<String, Map<Integer, Double>> fkLeftJoinInFileNullProbability) {
        this.fkLeftJoinInFileNullProbability = fkLeftJoinInFileNullProbability;
    }

    public int getLeftOuterJoinTag() {
        return leftOuterJoinTag;
    }

    public boolean hasLeftOuterJoin() {
        return constrainNullProbability.size() != 0;
    }

    public Map<Integer, Double> getConstrainNullProbability() {
        return constrainNullProbability;
    }

    public void setWriteOutJoinTable(String joinTableOutputPath) {
        if (leftOuterJoinTag != 0) {
            logger.info("start write thread");
            this.pkJoinInfoFileSize = new HashMap<>();
            this.writeOutJoinTable = new WriteOutJoinTable(joinTableOutputPath);
            TouchStoneThreadPool.getThreadPoolExecutor().submit(writeOutJoinTable);
        }
    }

    public boolean hasLeftOuterJoinFk() {
        return fkLeftJoinTag != null;
    }

    public boolean hasFkInFile() {
        return fkLeftJoinInFileNullProbability != null;
    }

    public void setFkLeftJoinInfoExitIndex(int startIndex, int stepLength) {
        fkLeftJoinInfoExistIndex = new HashMap<>();
        logger.info("startIndex is" + startIndex + ",stepLength is " + stepLength);
        for (String rpkName : fkLeftJoinTag.keySet()) {
            Map<Integer, Integer> mapSize = new HashMap<>();
            for (Entry<Integer, ArrayList<long[]>> joinInfo : fksJoinInfo.get(rpkName).entrySet()) {
                int index = joinInfo.getValue().size() - startIndex - 1;
                if (index > 0) {
                    mapSize.put(joinInfo.getKey(), index);
                }
            }
            fkLeftJoinInfoExistIndex.put(rpkName, mapSize);
        }
        fkStepLength = stepLength;
    }

    public void setReadOutJoinTable(String joinTableOutputPath, int threadId, Map<String, Integer> leftJoinTags) {
        if (fkLeftJoinInFileNullProbability != null) {
            fkReadOutJoinTables = new HashMap<>();
            for (String pkName : fkLeftJoinInFileNullProbability.keySet()) {
                String tableName = pkName.substring(1).split("\\.")[0];
                if (leftJoinTags.containsKey(tableName)) {
                    ReadOutJoinTable readOutJoinTable = new ReadOutJoinTable(
                            joinTableOutputPath + tableName + "_" + threadId + "_",
                            fkLeftJoinInFileNullProbability.get(pkName), leftJoinTags.get(tableName));
                    TouchStoneThreadPool.getThreadPoolExecutor().submit(readOutJoinTable);
                    fkReadOutJoinTables.put(pkName, readOutJoinTable);
                }
            }
        }
    }

    public void stopAllFileThread() {
        if (writeOutJoinTable != null) {
            writeOutJoinTable.stopThread();
        }
        if (fkReadOutJoinTables != null) {
            for (Entry<String, ReadOutJoinTable> stringReadOutJoinTableEntry : fkReadOutJoinTables.entrySet()) {
                logger.info(stringReadOutJoinTableEntry.getKey() + " " + stringReadOutJoinTableEntry.getValue().stopThread());
            }
        }
    }

    public Map<Integer, Long> getPkJoinInfoFileSize() {
        return pkJoinInfoFileSize;
    }

    public void init() {
        logger = Logger.getLogger(Touchstone.class);
        logger.debug("\n\tStart the initialization of table " + tableName);

        fksJoinInfoSizeMap = new HashMap<>();
        satisfiedFkJoinInfo = new ArrayList<>();

        for (Entry<String, Map<Integer, ArrayList<long[]>>> entry : fksJoinInfo.entrySet()) {
            fksJoinInfoSizeMap.put(entry.getKey(), new ArrayList<>());
            for (Entry<Integer, ArrayList<long[]>> entry2 : entry.getValue().entrySet()) {
                fksJoinInfoSizeMap.get(entry.getKey()).add(
                        new JoinStatusesSizePair(entry2.getKey(), entry2.getValue().size()));
            }
        }
        logger.debug("\nThe fksJoinInfoSizeMap is: " + fksJoinInfoSizeMap);

        pkJoinInfo = new HashMap<>();
        pkJoinInfoSizeMap = new HashMap<>();

        // we only support the equi-join on primary key and foreign key
        // if the primary key is a combination of foreign keys, the cardinality of the
        // intermediate join result may be larger in current implementation
        pkStrArr = pkStr.substring(1, pkStr.length() - 1).replaceAll(" ", "").split(",");

        rpkStrToArray = new HashMap<>();
        for (String rpkStr : referencedKeys) {
            String[] rpkStrArr = rpkStr.substring(1, rpkStr.length() - 1).replaceAll(" ", "").split(",");
            rpkStrToArray.put(rpkStr, rpkStrArr);
        }

        attributeValueMap = new HashMap<>();
        fkJoinStatusesMap = new HashMap<>();

        dateSdf = new SimpleDateFormat("yyyy-MM-dd");
        dateTimeSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // initialize the attribute 'accumulativeProbability' of 'FKJoin' node for 'adjustFksGeneStrategy'
        for (ConstraintChain constraintChain : constraintChains) {
            float accumulativeProbability = 1;
            List<CCNode> nodes = constraintChain.getNodes();
            for (CCNode node : nodes) {
                int type = node.getType();
                switch (type) {
                    case 0:
                        accumulativeProbability *= ((Filter) node.getNode()).getProbability();
                        break;
                    case 1:
                        // 'PKJoin' node must be at the end of the constraint chain
                        break;
                    case 2:
                    case 3:
                        FKJoin fkJoin = (FKJoin) node.getNode();
                        fkJoin.setAccumulativeProbability(accumulativeProbability);
                        accumulativeProbability *= fkJoin.getProbability();
                        break;
                    default:
                        logger.error("error fkJoin Type");
                }
            }
        }

        // we adjust the generation strategy of foreign keys according to
        // the join information of corresponding referenced primary keys
        for (int i = 0; i < shuffleMaxNum; i++) {
            boolean isSuccessful = adjustFksGeneStrategy();
            if (isSuccessful) {
                break;
            } else {
                Collections.shuffle(constraintChains);
                logger.debug("\n\tShuffle the constraint chains!, and the number of times is " + i);
            }
        }
        logger.info("\n\t The number of rules in constraint chains:" + getRulesNum());

        // initialize the 'parsii' for all basic filter operations (FilterOperation)
        initParsii();
    }

    // generate a tuple
    public String[] geneTuple(long uniqueNum) {
        String[] tuple = new String[keys.size() + attributes.size()];

        // clear the information of last generated tuple
        attributeValueMap.clear();
        fkJoinStatusesMap.clear();
        fkOutJoinStatusesMap.clear();
        // combined join statuses of the primary key
        int pkJoinStatuses = 0;

        // generate all non-key attributes
        for (Attribute attribute : attributes) {
            tuple[attribute.getIndex()] = attribute.geneData();
            attributeValueMap.put(attribute.getAttrName(), tuple[attribute.getIndex()]);
        }

        int uniqueKeyIndex = 0;
        for (Key key : keys) {
            if (key.getIndex() == -1) {
                uniqueKeyIndex = -1;
                break;
            }
        }

        // for Date and DateTime typed attributes, convert their values from long form to string form
        for (int i = 0; i < attributes.size(); i++) {
            if(tuple[keys.size() + i + uniqueKeyIndex]!=null){
                if ("date".equals(attributes.get(i).getDataType())) {
                    tuple[keys.size() + i + uniqueKeyIndex] = dateSdf.format(new Date(Long.parseLong(tuple[keys.size() + i + uniqueKeyIndex])));
                } else if ("datetime".equals(attributes.get(i).getDataType())) {
                    tuple[keys.size() + i + uniqueKeyIndex] = dateTimeSdf.format(new Date(Long.parseLong(tuple[keys.size() + i + uniqueKeyIndex])));
                }
            }
        }


        // set the unique number to its location
        for (Key key : keys) {
            if (key.getKeyType() == 0) {
                // record for an add column for no unique primary key
                if (key.getIndex() == -1) {
                    tuple[tuple.length - 1] = String.valueOf(uniqueNum);
                    attributeValueMap.put(key.getKeyName(), tuple[tuple.length - 1]);
                } else {
                    tuple[key.getIndex()] = String.valueOf(uniqueNum);
                    attributeValueMap.put(key.getKeyName(), tuple[key.getIndex()]);
                    // There is only one column with unique number in TPC-H,
                    // but it's not for SSB (lineorder: lo_orderkey & lo_linenumber)
                    // ------
                    // break;
                }
            }
        }

        // get the (combined) join statuses of primary key and foreign keys
        for (ConstraintChain constraintChain : constraintChains) {
            List<CCNode> nodes = constraintChain.getNodes();
            boolean flag = true;
            for (int j = 0; j < nodes.size(); j++) {
                int type = nodes.get(j).getType();
                switch (type) {
                    case 0:
                        // if the 'Filter' node is at the end of constraint chain, we can ignore it
                        if (j == nodes.size() - 1) {
                            continue;
                        }
                        Filter filter = (Filter) nodes.get(j).getNode();
                        if (!filter.isSatisfied(attributeValueMap)) {
                            // 'flag = false' indicates that the join statues of all following 'PKJoin's is false
                            //  and the data (tuple) can't flow to following 'FKJoin's
                            flag = false;
                        }
                        break;
                    case 1:
                        PKJoin pkJoin = (PKJoin) nodes.get(j).getNode();
                        // only one primary key -> only one variable (pkJoinStatuses)
                        if (flag) { // can join
                            for (int k = 0; k < pkJoin.getCanJoinNum().length; k++) {
                                pkJoinStatuses += pkJoin.getCanJoinNum()[k];
                            }
                        } else { // can't join
                            for (int k = 0; k < pkJoin.getCantJoinNum().length; k++) {
                                pkJoinStatuses += pkJoin.getCantJoinNum()[k];
                            }
                        }
                        break;
                    case 2:
                        //for right out join
                    case 3:
                        // the tuple can flow to current node
                        FKJoin fkJoin = (FKJoin) nodes.get(j).getNode();
                        if (flag) {
                            int numCount = 0;
                            //record num count for left join
                            int fkNumCount = 0;
                            if (fkJoinStatusesMap.containsKey(fkJoin.getRpkStr())) {
                                numCount = fkJoinStatusesMap.get(fkJoin.getRpkStr());
                            }
                            // can join
                            if (fkJoin.canJoin()) {
                                numCount += fkJoin.getCanJoinNum();
                                fkNumCount += fkJoin.getCanJoinNum();
                            }
                            // can't join
                            else {
                                numCount += fkJoin.getCantJoinNum();
                                fkNumCount += fkJoin.getCantJoinNum();
                                //if it is right join,don't change flag
                                if (type != 3) {
                                    flag = false;
                                }
                            }

                            fkJoinStatusesMap.put(fkJoin.getRpkStr(), numCount);
                            if (fkLeftJoinTag != null && fkLeftJoinTag.containsKey(fkJoin.getRpkStr())) {
                                if (fkOutJoinStatusesMap.containsKey(fkJoin.getRpkStr())) {
                                    fkOutJoinStatusesMap.put(fkJoin.getRpkStr(),
                                            fkNumCount + fkOutJoinStatusesMap.get(fkJoin.getRpkStr()));
                                } else {
                                    fkOutJoinStatusesMap.put(fkJoin.getRpkStr(), fkNumCount);
                                }
                            }
                        } else {
                            if (fkLeftJoinTag != null && fkLeftJoinTag.containsKey(fkJoin.getRpkStr())) {
                                int joinNum = fkJoin.getCantJoinNum();
                                if ((fkLeftJoinTag.get(fkJoin.getRpkStr()) & joinNum) == joinNum) {
                                    if (fkOutJoinStatusesMap.containsKey(fkJoin.getRpkStr())) {
                                        fkOutJoinStatusesMap.put(fkJoin.getRpkStr(),
                                                fkOutJoinStatusesMap.get(fkJoin.getRpkStr()) + joinNum);
                                    } else {
                                        fkOutJoinStatusesMap.put(fkJoin.getRpkStr(), joinNum);
                                    }
                                }
                            }
                        }
                        break;
                    default:
                        logger.error("CCNode Type Error");
                } // switch
            } // for nodes
        } // for chains

        // generate left join keys from file
        if (fkReadOutJoinTables != null) {
            for (Entry<String, Integer> joinInfo : fkOutJoinStatusesMap.entrySet()) {
                if (fkReadOutJoinTables.containsKey(joinInfo.getKey())) {
                    long[] fkValues = fkReadOutJoinTables.get(joinInfo.getKey()).getJoinKey(joinInfo.getValue());
                    if (fkValues == null) {
                        continue;
                    }
                    String[] rpkNames = rpkStrToArray.get(joinInfo.getKey());
                    for (int i = 0; i < rpkNames.length; i++) {
                        attributeValueMap.put(referKeyForeKeyMap.get(rpkNames[i]), fkValues[i] + "");
                    }
                    fkJoinStatusesMap.remove(joinInfo.getKey());
                    fkOutJoinStatusesMap.remove(joinInfo.getKey());
                }
            }
        }


        //generate left join keys from memory
        for (Entry<String, Integer> entry : fkOutJoinStatusesMap.entrySet()) {
            int fkNumCount = entry.getValue();
            int maxSize = 0;
            int fkOuterStatus = 0;
            for (Entry<Integer, Integer> existIndex : fkLeftJoinInfoExistIndex.get(entry.getKey()).entrySet()) {
                if ((existIndex.getKey() & fkNumCount) == fkNumCount) {
                    if (existIndex.getValue() > maxSize) {
                        fkOuterStatus = existIndex.getKey();
                        maxSize = existIndex.getValue();
                    }
                }
            }
            if (maxSize > 0) {
                ArrayList<long[]> candidates = fksJoinInfo.get(entry.getKey()).get(fkOuterStatus);
                int randomIndex = (int) (maxSize / fkStepLength * Math.random());
                Collections.swap(candidates, maxSize - randomIndex * fkStepLength, maxSize);
                fkLeftJoinInfoExistIndex.get(entry.getKey()).put(fkOuterStatus, maxSize - fkStepLength);
                long[] fkValues = candidates.get(maxSize);
                String[] rpkNames = rpkStrToArray.get(entry.getKey());
                for (int j = 0; j < rpkNames.length; j++) {
                    attributeValueMap.put(referKeyForeKeyMap.get(rpkNames[j]), fkValues[j] + "");
                }
                fkJoinStatusesMap.remove(entry.getKey());
                fkOutJoinStatusesMap.remove(entry.getKey());
            }
        }


        // generate foreign keys
        // currently, we don't consider the situation of multiple assignments
        //     (under mixed reference) to the foreign key
        // TODO
        for (Entry<String, Integer> entry : fkJoinStatusesMap.entrySet()) {
            int numCount = entry.getValue();
            ArrayList<JoinStatusesSizePair> joinStatusesSizePairs = fksJoinInfoSizeMap.get(entry.getKey());
            satisfiedFkJoinInfo.clear();
            // accumulative (*) size
            int cumulant = 0;
            for (JoinStatusesSizePair statusesSizePair : joinStatusesSizePairs) {
                if ((statusesSizePair.getJoinStatuses() & numCount) == numCount) {
                    cumulant += statusesSizePair.getSize();
                    satisfiedFkJoinInfo.add(new JoinStatusesSizePair(
                            statusesSizePair.getJoinStatuses(), cumulant));
                }
            }

            if (cumulant == 0) {
                logger.error("\n\tfkMissCount: " + Statistic.fkMissCount.incrementAndGet() +
                        ", referenced primary key: " + entry.getKey() + ", numCount: " + numCount);
                return tuple;
            }

            // in fact, the information here (fksJoinInfo) has been compressed, so it can not be done completely random
            ArrayList<long[]> candidates;
            cumulant = (int) (Math.random() * cumulant);
            for (JoinStatusesSizePair joinStatusesSizePair : satisfiedFkJoinInfo) {
                if (cumulant < joinStatusesSizePair.getSize()) {
                    candidates = fksJoinInfo.get(entry.getKey()).get(joinStatusesSizePair.getJoinStatuses());
                    long[] fkValues = candidates.get((int) (Math.random() * candidates.size()));
                    String[] rpkNames = rpkStrToArray.get(entry.getKey());
                    for (int j = 0; j < rpkNames.length; j++) {
                        attributeValueMap.put(referKeyForeKeyMap.get(rpkNames[j]), fkValues[j] + "");
                    }
                    break;
                }
            }
        }

        for (Key key : keys) {
            if (key.getKeyType() == 1) { // foreign key
                tuple[key.getIndex()] = attributeValueMap.get(key.getKeyName());
            }
        }


        // maintain the combined join statuses of the primary key
        String[] pkNames = pkStrArr;
        long[] pkValues = new long[pkNames.length];
        for (int i = 0; i < pkNames.length; i++) {
            if (attributeValueMap.get(pkNames[i]) != null) {
                pkValues[i] = Long.parseLong(attributeValueMap.get(pkNames[i]));
            }

        }
        if (!pkJoinInfo.containsKey(pkJoinStatuses)) {
            pkJoinInfo.put(pkJoinStatuses, new ArrayList<>());
            pkJoinInfoSizeMap.put(pkJoinStatuses, 0L);
        }
        // compression algorithm
        ArrayList<long[]> candidates = pkJoinInfo.get(pkJoinStatuses);
        long size = pkJoinInfoSizeMap.get(pkJoinStatuses) + 1;
        pkJoinInfoSizeMap.put(pkJoinStatuses, size);
        if (candidates.size() < pkvsMaxSize) {
            candidates.add(pkValues);
        } else {
            if (Math.random() < ((double) pkvsMaxSize / size)) {
                pkValues = candidates.set((int) (Math.random() * candidates.size()), pkValues);
            }
            if (leftOuterJoinTag != 0) {
                int leftCanNotJoin = 2 * leftOuterJoinTag;
                if ((pkJoinStatuses & leftCanNotJoin) != leftCanNotJoin) {
                    if (!pkJoinInfoFileSize.containsKey(pkJoinStatuses)) {
                        pkJoinInfoFileSize.put(pkJoinStatuses, 0L);
                    }
                    pkJoinInfoFileSize.put(pkJoinStatuses, pkJoinInfoFileSize.get(pkJoinStatuses) - 1);
                    writeOutJoinTable.write(pkJoinStatuses, pkValues);
                } else {
                    logger.info("have not written in file:" + pkJoinStatuses + " " + leftCanNotJoin);
                }
            }
        }

        return tuple;
    }

    // adjust the generation strategy of foreign keys according to the join information of referenced primary keys
    // if return is true, the adjustment is successful! Otherwise, it's fail!
    private boolean adjustFksGeneStrategy() {
        for (String referencedKey : referencedKeys) {
            // get all 'FKJoin' nodes associated with current foreign key
            List<FKJoin> fkJoinNodes = new ArrayList<>();
            for (ConstraintChain constraintChain : constraintChains) {
                List<CCNode> nodes = constraintChain.getNodes();
                for (CCNode node : nodes) {
                    if (node.getType() == 2 || node.getType() == 3) {
                        FKJoin fkJoin = (FKJoin) node.getNode();
                        if (fkJoin.getRpkStr().equals(referencedKey)) {
                            fkJoinNodes.add(fkJoin);
                        }
                    }
                }
            }
            logger.debug("\nAll 'FKJoin' nodes of " + referencedKey + fkJoinNodes);

            // all 'FKJoinAdjustment' only share one array 'joinStatuses'
            boolean[] joinStatuses = new boolean[fkJoinNodes.size()];

            // set the 'fkJoinAdjustment' for every 'FKJoin' node
            for (int j = 0; j < fkJoinNodes.size(); j++) {
                //todo the node only pknode not need to adjust
                // we don't need to adjust the generation strategy of the first 'FKJoin' node
                if (j == 0) {
                    fkJoinNodes.get(0).setFkJoinAdjustment(new FKJoinAdjustment(0, joinStatuses,
                            new ArrayList<>(), fkJoinNodes.get(0).getProbability()));
                    continue;
                }

                List<FKJoinAdjustRule> rules = getRules(fkJoinNodes, j);
                float probability = getProbability(fkJoinNodes, rules, j);

                if (probability < 0 || probability > 1) {
                    logger.error("probability is " + probability + ", adjustment is fail!");
                    return false;
                } else {
                    FKJoinAdjustment fkJoinAdjustment = new FKJoinAdjustment(j, joinStatuses, rules, probability);
                    fkJoinNodes.get(j).setFkJoinAdjustment(fkJoinAdjustment);
                    logger.debug("\n\tAdjustment of fkJoins " + j + ": " + fkJoinAdjustment);
                }
            }
        }
        return true;
    }

    // order >= 1
    // we don't need to adjust the generation strategy of the first 'FKJoin' node (order == 0)
    private List<FKJoinAdjustRule> getRules(List<FKJoin> fkJoinNodes, int order) {
        List<FKJoinAdjustRule> rules = new ArrayList<>();
        // the number of all possible join statues
        int joinStatusesNum = (int) Math.pow(2, order + 1);
        for (int i = 0; i < joinStatusesNum; i++) {
            // the foreign key is likely to have no join status, so there should be three states (True, False, None)
            // TODO
            // i -> joinStatuses
            String str = new StringBuilder(Integer.toBinaryString(i)).reverse().toString();
            boolean[] joinStatuses = new boolean[order + 1];
            for (int j = 0; j < str.length(); j++) {
                joinStatuses[j] = str.charAt(j) == '1';
            }
            for (int j = str.length(); j < order + 1; j++) {
                joinStatuses[j] = false;
            }

            // joinStatuses -> numCount (combined join statuses)
            int numCount = 0;
            for (int j = 0; j < order + 1; j++) {
                if (joinStatuses[j]) {
                    numCount += fkJoinNodes.get(j).getCanJoinNum();
                } else {
                    numCount += fkJoinNodes.get(j).getCantJoinNum();
                }
            }

            // generate the rule according to the existence of 'numCount' (combined join statuses)
            String rpkStr = fkJoinNodes.get(0).getRpkStr();
            ArrayList<JoinStatusesSizePair> joinStatusesSizePairs = fksJoinInfoSizeMap.get(rpkStr);
            boolean existent = false;
            for (JoinStatusesSizePair joinStatusesSizePair : joinStatusesSizePairs) {
                if ((joinStatusesSizePair.getJoinStatuses() & numCount) == numCount) {
                    existent = true;
                    break;
                }
            }
            if (!existent) {
                rules.add(new FKJoinAdjustRule(joinStatuses));
            }
        }

        // remove invalid rules which have the same cause
        Collections.sort(rules);
        for (int i = 0; i < rules.size(); i++) {
            if (i == rules.size() - 1) {
                break;
            }
            if (Arrays.toString(rules.get(i).getCause()).equals(Arrays.toString(rules.get(i + 1).getCause()))) {
                rules.remove(i);
                rules.remove(i);
                i = i - 1;
            }
        }
        return rules;
    }

    // get the 'probability' of can-join situation (join status is True) after the adjustment
    private float getProbability(List<FKJoin> fkJoinNodes, List<FKJoinAdjustRule> rules, int order) {
        // every rule in 'rules' consumes partial 'true probability' or 'false probability' of current 'FKJoin' node
        float trueProbability = 0, falseProbability = 0;
        for (FKJoinAdjustRule rule : rules) {
            boolean[] cause = rule.getCause();
            float probabilityOfCause = 1;
            for (int j = 0; j < cause.length; j++) {
                boolean[] frontPartCause = Arrays.copyOf(cause, cause.length - j);
                FKJoin frontFkJoin = fkJoinNodes.get(cause.length - j - 1);
                List<FKJoinAdjustRule> frontFkJoinRules = frontFkJoin.getFkJoinAdjustment().getRules();

                boolean flag = false;
                for (FKJoinAdjustRule frontFkJoinRule : frontFkJoinRules) {
                    boolean[] causeAndEffect = new boolean[frontPartCause.length];
                    System.arraycopy(frontFkJoinRule.getCause(), 0,
                            causeAndEffect, 0, frontPartCause.length - 1);
                    causeAndEffect[causeAndEffect.length - 1] = frontFkJoinRule.getEffect();
                    if (Arrays.equals(frontPartCause, causeAndEffect)) {
                        flag = true;
                        break;
                    }
                }

                if (!flag) {
                    float accumulativeProbability = frontFkJoin.getAccumulativeProbability();
                    FKJoinAdjustment fkJoinAdjustment = frontFkJoin.getFkJoinAdjustment();
                    if (frontPartCause[frontPartCause.length - 1]) {
                        probabilityOfCause *= (accumulativeProbability * fkJoinAdjustment.getProbability());
                    } else {
                        probabilityOfCause *= (accumulativeProbability * (1 - fkJoinAdjustment.getProbability()));
                    }
                }
            } // for cause.length

            if (rule.getEffect()) {
                trueProbability += probabilityOfCause;
            } else {
                falseProbability += probabilityOfCause;
            }
        } //for rules

        // may be wrong! (there may be dependencies between filters)
        // TODO
        FKJoin fkJoin = fkJoinNodes.get(order);
        float originalTrueProbability = fkJoin.getAccumulativeProbability() * fkJoin.getProbability();
        float originalFalseProbability = fkJoin.getAccumulativeProbability() * (1 - fkJoin.getProbability());
        return (originalTrueProbability - trueProbability) /
                ((originalTrueProbability - trueProbability) + (originalFalseProbability - falseProbability));
    }

    // call the function 'initParsii' of all 'FilterOpertion's
    // facilitate all 'FilterOpertion's with the ability to determine whether they are satisfied
    private void initParsii() {
        for (ConstraintChain constraintChain : constraintChains) {
            List<CCNode> nodes = constraintChain.getNodes();
            for (CCNode node : nodes) {
                if (node.getType() == 0) {
                    Filter filter = (Filter) node.getNode();
                    FilterOperation[] operations = filter.getFilterOperations();
                    for (FilterOperation operation : operations) {
                        operation.initParsii(parameterMap.get(operation.getId()), attributeMap);
                    }
                }
            }
        }
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableSize() {
        return tableSize;
    }

    public String getPkStr() {
        return pkStr;
    }

    public List<String> getReferencedKeys() {
        return referencedKeys;
    }

    public Map<Integer, ArrayList<long[]>> getPkJoinInfo() {
        return pkJoinInfo;
    }

    @Override
    public String toString() {
        return "\nTableGeneTemplate [tableName=" + tableName + ", tableSize=" + tableSize + ", pkStr=" + pkStr
                + ", \nkeys=" + keys + ", \nattributes=" + attributes + ", \nconstraintChains=" + constraintChains
                + ", \nreferencedKeys=" + referencedKeys + ", \nreferKeyForeKeyMap=" + referKeyForeKeyMap
                + ", \nparameterMap=" + parameterMap + ", \nshuffleMaxNum=" + shuffleMaxNum + ", pkvsMaxSize="
                + pkvsMaxSize + "]";
    }

    // for experiments
    // for obtaining the number of constraint chains
    public int getConstraintChainsNum() {
        return constraintChains.size();
    }

    // for obtaining the number of constraints in constraint chains
    public int getConstraintsNum() {
        int count = 0;
        for (ConstraintChain constraintChain : constraintChains) {
            count += constraintChain.getNodes().size();
        }
        return count;
    }

    // for obtaining the number of entries in the join information table
    public int getEntriesNum() {
        int count = 0;
        for (Entry<String, Map<Integer, ArrayList<long[]>>> stringMapEntry : fksJoinInfo.entrySet()) {
            int tmp = stringMapEntry.getValue().size();
            if (tmp > count) {
                count = tmp;
            }
        }
        return count;
    }

    // for obtaining the number of rules in constraint chains
    private int getRulesNum() {
        int count = 0;
        for (ConstraintChain constraintChain : constraintChains) {
            List<CCNode> nodes = constraintChain.getNodes();
            for (CCNode node : nodes) {
                if (node.getType() == 2 || node.getType() == 3) {
                    FKJoin fkJoin = (FKJoin) node.getNode();
                    try {
                        count += fkJoin.getFkJoinAdjustment().getRules().size();
                    }catch (Exception e){
                    }
                }
            }
        }
        return count;
    }

    public Map<String, Integer> getFkJoinStatus() {
        return fkJoinStatus;
    }
}

class Key implements Serializable {

    private static final long serialVersionUID = 1L;

    private String keyName;
    // 0: it's generated sequentially (uniqueness needs to be guaranteed)
    // 1: it's generated according to the its join statuses (it must be a foreign key)
    private int keyType;

    private int index;

    Key(String keyName, int keyType, int index) {
        super();
        this.keyName = keyName;
        this.keyType = keyType;
        this.index = index;
    }

    Key(Key key) {
        super();
        this.keyName = key.keyName;
        this.keyType = key.keyType;
        this.index = key.index;
    }

    public int getIndex() {
        return index;
    }

    public String getKeyName() {
        return keyName;
    }

    public int getKeyType() {
        return keyType;
    }

    @Override
    public String toString() {
        return "Key [keyName=" + keyName + ", keyType=" + keyType + "]";
    }
}

class JoinStatusesSizePair {

    // combined join statuses
    private int joinStatuses;
    // the size of the primary keys that satisfy the combined join statuses
    private int size;

    JoinStatusesSizePair(int joinStatuses, int size) {
        super();
        this.joinStatuses = joinStatuses;
        this.size = size;
    }

    public JoinStatusesSizePair(JoinStatusesSizePair joinStatusesSizePair) {
        super();
        this.joinStatuses = joinStatusesSizePair.joinStatuses;
        this.size = joinStatusesSizePair.size;
    }

    public int getJoinStatuses() {
        return joinStatuses;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "\n\tJoinStatusesSizePair [joinStatuses=" + joinStatuses + ", size="
                + size + "]";
    }
}
