package edu.ecnu.touchstone.pretreatment;

import com.joptimizer.exception.JOptimizerException;
import edu.ecnu.touchstone.constraintchain.*;
import edu.ecnu.touchstone.queryinstantiation.Parameter;
import edu.ecnu.touchstone.run.Statistic;
import edu.ecnu.touchstone.run.Touchstone;
import edu.ecnu.touchstone.schema.Attribute;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

// the generation template of the table
// each thread has such an object
// at one node, all threads can share 'fksJoinInfo' (reduce the memory consumption)
public class TableGeneTemplate implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tableName = null;
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

    // 每个主键列null的概率
    private Map<Integer, Double> keyNullProbability;

    // Join table中每个status的null概率
    private Map<Integer, Double> tableNullProbability;

    // 参照表中涉及到左外连接的status
    private Map<String, Map<Integer, Integer[]>> fksNullInfo;

    // fkJoinTable中非重复值的位置
    private Map<String, List<JoinStatusesSizePair>> fksJoinIndex;

    private Map<String,Integer> fkIndexSize;

    private long currentTableSize;

    public void setFksNullInfo(Map<String, Map<Integer, Integer[]>> fksNullInfo) {
        this.fksNullInfo = fksNullInfo;
    }


    public TableGeneTemplate(String tableName, long tableSize, String pkStr, List<Key> keys, List<Attribute> attributes,
                             List<ConstraintChain> constraintChains, List<String> referencedKeys, Map<String, String> referKeyForeKeyMap,
                             Map<Integer, Parameter> parameterMap, Map<String, Attribute> attributeMap, int shuffleMaxNum,
                             int pkvsMaxSize, Map<Integer, Double> keyNullProbability) {
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
        this.keyNullProbability = keyNullProbability;
    }

    // map: Key: the string representation of referenced primary key (support mixed reference)
    //      Value: Key: combined join statuses
    //             Value: a list of primary keys that satisfy the combined join statuses
    private Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo = null;

    // it's set by Controller according to 'referencedKeys'
    public void setFksJoinInfo(Map<String, Map<Integer, ArrayList<long[]>>> fksJoinInfo) {
        this.fksJoinInfo = fksJoinInfo;
    }

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

    // combined join statuses of the primary key
    private transient int pkJoinStatuses;

    private transient Logger logger = null;
    private transient SimpleDateFormat dateSdf = null;
    private transient SimpleDateFormat dateTimeSdf = null;

    public void init() {
        logger = Logger.getLogger(Touchstone.class);
        logger.debug("\n\tStart the initialization of table " + tableName);

        fksJoinInfoSizeMap = new HashMap<String, ArrayList<JoinStatusesSizePair>>();
        satisfiedFkJoinInfo = new ArrayList<JoinStatusesSizePair>();

        Iterator<Entry<String, Map<Integer, ArrayList<long[]>>>> iterator =
                fksJoinInfo.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Map<Integer, ArrayList<long[]>>> entry = iterator.next();
            fksJoinInfoSizeMap.put(entry.getKey(), new ArrayList<JoinStatusesSizePair>());
            Iterator<Entry<Integer, ArrayList<long[]>>> iterator2 = entry.getValue().entrySet().iterator();
            while (iterator2.hasNext()) {
                Entry<Integer, ArrayList<long[]>> entry2 = iterator2.next();
                fksJoinInfoSizeMap.get(entry.getKey()).add(
                        new JoinStatusesSizePair(entry2.getKey(), entry2.getValue().size()));
            }
            fksJoinInfoSizeMap.get(entry.getKey()).sort(Comparator.comparing(JoinStatusesSizePair::getJoinStatuses));
        }
        logger.debug("\nThe fksJoinInfoSizeMap is: " + fksJoinInfoSizeMap);

        fksJoinIndex = new HashMap<>();
        fkIndexSize = new HashMap<>();
        for (Entry<String, Map<Integer, ArrayList<long[]>>> joinInfos : fksJoinInfo.entrySet()) {
            List<JoinStatusesSizePair> joinIndex = new ArrayList<>(fksJoinInfoSizeMap.get(joinInfos.getKey()));
            int size=0;
            for (JoinStatusesSizePair index : joinIndex) {
                size+=index.getSize();
            }
            fksJoinIndex.put(joinInfos.getKey(), joinIndex);
            fkIndexSize.put(joinInfos.getKey(),size);
        }

        pkJoinInfo = new HashMap<Integer, ArrayList<long[]>>();
        pkJoinInfoSizeMap = new HashMap<Integer, Long>();

        // we only support the equi-join on primary key and foreign key
        // if the primary key is a combination of foreign keys, the cardinality of the
        // intermediate join result may be larger in current implementation
        pkStrArr = pkStr.substring(1, pkStr.length() - 1).replaceAll(" ", "").split(",");

        rpkStrToArray = new HashMap<String, String[]>();
        for (int i = 0; i < referencedKeys.size(); i++) {
            String rpkStr = referencedKeys.get(i);
            String[] rpkStrArr = rpkStr.substring(1, rpkStr.length() - 1).replaceAll(" ", "").split(",");
            rpkStrToArray.put(rpkStr, rpkStrArr);
        }

        attributeValueMap = new HashMap<String, String>();
        fkJoinStatusesMap = new HashMap<String, Integer>();

        dateSdf = new SimpleDateFormat("yyyy-MM-dd");
        dateTimeSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // initialize the attribute 'accumulativeProbability' of 'FKJoin' node for 'adjustFksGeneStrategy'
        for (int i = 0; i < constraintChains.size(); i++) {
            float accumulativeProbability = 1;
            List<CCNode> nodes = constraintChains.get(i).getNodes();
            for (int j = 0; j < nodes.size(); j++) {
                int type = nodes.get(j).getType();
                switch (type) {
                    case 0:
                        accumulativeProbability *= ((Filter) nodes.get(j).getNode()).getProbability();
                        break;
                    case 1:
                        // 'PKJoin' node must be at the end of the constraint chain
                        break;
                    case 2:
                        FKJoin fkJoin = (FKJoin) nodes.get(j).getNode();
                        fkJoin.setAccumulativeProbability(accumulativeProbability);
                        accumulativeProbability *= fkJoin.getProbability();
                        break;
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

    public TableGeneTemplate(TableGeneTemplate template) {
        super();
        this.tableName = template.tableName;
        this.tableSize = template.tableSize;
        this.pkStr = template.pkStr;
        this.keys = new ArrayList<Key>();
        for (int i = 0; i < template.keys.size(); i++) {
            this.keys.add(new Key(template.keys.get(i)));
        }
        this.attributes = new ArrayList<Attribute>();
        for (int i = 0; i < template.attributes.size(); i++) {
            this.attributes.add(new Attribute(template.attributes.get(i)));
        }
        this.constraintChains = new ArrayList<ConstraintChain>();
        for (int i = 0; i < template.constraintChains.size(); i++) {
            this.constraintChains.add(new ConstraintChain(template.constraintChains.get(i)));
        }
        this.referencedKeys = new ArrayList<String>();
        this.referencedKeys.addAll(template.referencedKeys);
        this.referKeyForeKeyMap = new HashMap<String, String>();
        this.referKeyForeKeyMap.putAll(template.referKeyForeKeyMap);
        this.parameterMap = new HashMap<Integer, Parameter>();
        for (Entry<Integer, Parameter> entry : template.parameterMap.entrySet()) {
            this.parameterMap.put(entry.getKey(), new Parameter(entry.getValue()));
        }
        this.attributeMap = new HashMap<String, Attribute>();
        for (Attribute attribute : this.attributes) {
            this.attributeMap.put(attribute.getAttrName(), attribute);
        }
        this.shuffleMaxNum = template.shuffleMaxNum;
        this.pkvsMaxSize = template.pkvsMaxSize;
        this.keyNullProbability = template.keyNullProbability;
        this.tableNullProbability = template.tableNullProbability;
        this.fksJoinIndex = template.fksJoinIndex;
        this.fksNullInfo = template.fksNullInfo;
        this.currentTableSize = template.currentTableSize;
        // shallow copy
        this.fksJoinInfo = template.fksJoinInfo;
        init();
    }

    // generate a tuple
    public String[] geneTuple(long uniqueNum) {
        String[] tuple = new String[keys.size() + attributes.size()];

        // clear the information of last generated tuple
        attributeValueMap.clear();
        fkJoinStatusesMap.clear();
        pkJoinStatuses = 0;
        int nullStatus = 0;

        // generate all non-key attributes
        for (int i = 0; i < attributes.size(); i++) {
            tuple[keys.size() + i] = attributes.get(i).geneData();
            attributeValueMap.put(attributes.get(i).getAttrName(), tuple[keys.size() + i]);
        }

        // set the unique number to its location
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).getKeyType() == 0) {
                tuple[i] = uniqueNum + "";
                attributeValueMap.put(keys.get(i).getKeyName(), tuple[i]);
                // There is only one column with unique number in TPC-H,
                // but it's not for SSB (lineorder: lo_orderkey & lo_linenumber)
                // ------
                // break;
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
                                if (pkJoin.getNullProbability()[k] != 0) {
                                    nullStatus += pkJoin.getCanJoinNum()[k];
                                }
                            }
                        } else { // can't join
                            for (int k = 0; k < pkJoin.getCantJoinNum().length; k++) {
                                pkJoinStatuses += pkJoin.getCantJoinNum()[k];
                                if (pkJoin.getNullProbability()[k] != 0) {
                                    nullStatus += pkJoin.getCantJoinNum()[k];
                                }
                            }
                        }

                        break;
                    case 2:
                        // the tuple can flow to current node
                        if (flag) {
                            FKJoin fkJoin = (FKJoin) nodes.get(j).getNode();
                            int numCount = 0;
                            if (fkJoinStatusesMap.containsKey(fkJoin.getRpkStr())) {
                                numCount = fkJoinStatusesMap.get(fkJoin.getRpkStr());
                            }
                            if (fkJoin.canJoin()) { // can join
                                numCount += fkJoin.getCanJoinNum();
                            } else { // can't join
                                numCount += fkJoin.getCantJoinNum();
                                if (fkJoin.getNullProbability() == 0) {
                                    flag = false;
                                }
                            }
                            fkJoinStatusesMap.put(fkJoin.getRpkStr(), numCount);
                        }
                        break;
                } // switch
            } // for nodes
        } // for chains

        // generate foreign keys
        // currently, we don't consider the situation of multiple assignments
        //     (under mixed reference) to the foreign key
        // TODO
        for (Entry<String, Integer> tupleJoinStatus : fkJoinStatusesMap.entrySet()) {
            int numCount = tupleJoinStatus.getValue();
            ArrayList<JoinStatusesSizePair> joinStatusesSizePairs = fksJoinInfoSizeMap.get(tupleJoinStatus.getKey());
            List<JoinStatusesSizePair> joinIndexesSizePairs= fksJoinIndex.get(tupleJoinStatus.getKey());
            satisfiedFkJoinInfo.clear();
            List<JoinStatusesSizePair> satisfiedFkJoinIndex=new ArrayList<>();
            // accumulative (*) size
            int cumulant = 0;
            for (int i = 0; i < joinStatusesSizePairs.size(); i++) {
                if ((joinStatusesSizePairs.get(i).getJoinStatuses() & numCount) == numCount) {
                    cumulant += joinStatusesSizePairs.get(i).getSize();
                    satisfiedFkJoinInfo.add(new JoinStatusesSizePair(
                            joinStatusesSizePairs.get(i).getJoinStatuses(), cumulant));
                    if(joinIndexesSizePairs.get(i).getSize()!=0){
                        satisfiedFkJoinIndex.add(joinIndexesSizePairs.get(i));
                    }
                }
            }

            if (cumulant == 0) {
                logger.error("\n\tfkMissCount: " + Statistic.fkMissCount.incrementAndGet() +
                        ", referenced primary key: " + tupleJoinStatus.getKey() + ", numCount: " + numCount);
                return tuple;
            }
            long[] fkValues = new long[0];
            boolean existFkNotJoin=satisfiedFkJoinIndex.size() != 0;
            //这里需要找一个合适的判断
            boolean randomOrNot = Math.random() >
                    (double)fkIndexSize.get(tupleJoinStatus.getKey()) /(this.tableSize - currentTableSize);
            if (existFkNotJoin && !randomOrNot) {
                JoinStatusesSizePair fkJoinIndexInfo=satisfiedFkJoinIndex
                        .get((int)(Math.random()*satisfiedFkJoinIndex.size()));
                ArrayList<long[]> candidates = fksJoinInfo.get(tupleJoinStatus.getKey()).
                        get(fkJoinIndexInfo.getJoinStatuses());
                int randomIndex = (int) (Math.random() * fkJoinIndexInfo.getSize());
                fkValues = candidates.get(randomIndex);
                candidates.set(randomIndex, candidates.get(fkJoinIndexInfo.subAndGetSize()));
                candidates.set(fkJoinIndexInfo.getSize(), fkValues);
                fkIndexSize.put(tupleJoinStatus.getKey(),fkIndexSize.get(tupleJoinStatus.getKey())-1);
            }else {
                // in fact, the information here (fksJoinInfo) has been compressed, so it can not be done completely random
                cumulant = (int) (Math.random() * cumulant);
                for (JoinStatusesSizePair joinStatusesSizePair : satisfiedFkJoinInfo) {
                    if (cumulant < joinStatusesSizePair.getSize()) {
                        ArrayList<long[]> candidates = fksJoinInfo.get(tupleJoinStatus.getKey()).
                                get(joinStatusesSizePair.getJoinStatuses());
                        fkValues = candidates.get((int) (Math.random() * candidates.size()));
                        break;
                    }
                }
            }
            String[] rpkNames = rpkStrToArray.get(tupleJoinStatus.getKey());
            for (int i = 0; i < rpkNames.length; i++) {
                attributeValueMap.put(referKeyForeKeyMap.get(rpkNames[i]), fkValues[i] + "");
            }
        }

        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).getKeyType() == 1) { // foreign key
                tuple[i] = attributeValueMap.get(keys.get(i).getKeyName());
            }
        }

        // maintain the combined join statuses of the primary key
        String[] pkNames = pkStrArr;
        long[] pkValues = new long[pkNames.length];
        for (int i = 0; i < pkNames.length; i++) {
            pkValues[i] = Long.parseLong(attributeValueMap.get(pkNames[i]));
        }
        if (!pkJoinInfo.containsKey(pkJoinStatuses)) {
            pkJoinInfo.put(pkJoinStatuses, new ArrayList<long[]>());
            pkJoinInfoSizeMap.put(pkJoinStatuses, 0L);
        }


        // compression algorithm
        ArrayList<long[]> candidates = pkJoinInfo.get(pkJoinStatuses);
        long size = pkJoinInfoSizeMap.get(pkJoinStatuses) + 1;
        pkJoinInfoSizeMap.put(pkJoinStatuses, size);
        if (keyNullProbability.size() == 0) {
            if (candidates.size() < pkvsMaxSize) {
                candidates.add(pkValues);
            } else {
                if (Math.random() < ((double) pkvsMaxSize / size)) {
                    candidates.set((int) (Math.random() * candidates.size()), pkValues);
                }
            }
        } else {
            if (tableNullProbability == null) {
                if(candidates.size() < pkvsMaxSize){
                    candidates.add(pkValues);
                }else {
                    computeNullProbability();
                }
            }else {
                if (Math.random() > tableNullProbability.get(pkJoinStatuses & nullStatus)) {
                    candidates.add(pkValues);
                }
            }
        }


        // for Date and DateTime typed attributes, convert their values from long form to string form
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getDataType().equals("date")) {
                tuple[keys.size() + i] = dateSdf.format(new Date(Long.parseLong(tuple[keys.size() + i])));
            } else if (attributes.get(i).getDataType().equals("datetime")) {
                tuple[keys.size() + i] = dateTimeSdf.format(new Date(Long.parseLong(tuple[keys.size() + i])));
            }
        }
        currentTableSize += 1;
        return tuple;
    }


    public Integer[] getNullStatuses() {
        if (keyNullProbability.size() == 0) {
            return null;
        } else {
            int statusSize = (int) Math.pow(2, keyNullProbability.size()) - 1;
            Integer[] nullStatuses = new Integer[statusSize];

            int[] checkType = new int[keyNullProbability.size()];
            checkType[0] = 1;
            for (int i = 1; i < checkType.length; i++) {
                checkType[i] = checkType[i - 1] * 2;
            }
            for (int i = 0; i < statusSize; i++) {
                int joinStatus = 0;
                int j = 0;
                for (Integer status : keyNullProbability.keySet()) {
                    if ((i & checkType[j]) == 0) {
                        joinStatus += status;
                    } else {
                        joinStatus += 2 * status;
                    }
                    j++;
                }
                nullStatuses[i] = joinStatus;
            }
            return nullStatuses;
        }
    }

    // adjust the generation strategy of foreign keys according to the join information of referenced primary keys
    // if return is true, the adjustment is successful! Otherwise, it's fail!
    private boolean adjustFksGeneStrategy() {
        for (int i = 0; i < referencedKeys.size(); i++) {
            // get all 'FKJoin' nodes associated with current foreign key
            List<FKJoin> fkJoinNodes = new ArrayList<FKJoin>();
            for (int j = 0; j < constraintChains.size(); j++) {
                List<CCNode> nodes = constraintChains.get(j).getNodes();
                for (int k = 0; k < nodes.size(); k++) {
                    if (nodes.get(k).getType() == 2) {
                        FKJoin fkJoin = (FKJoin) nodes.get(k).getNode();
                        if (fkJoin.getRpkStr().equals(referencedKeys.get(i))) {
                            fkJoinNodes.add(fkJoin);
                        }
                    }
                }
            }
            logger.debug("\nAll 'FKJoin' nodes of " + referencedKeys.get(i) + fkJoinNodes);

            // all 'FKJoinAdjustment' only share one array 'joinStatuses'
            boolean[] joinStatuses = new boolean[fkJoinNodes.size()];

            // set the 'fkJoinAdjustment' for every 'FKJoin' node
            for (int j = 0; j < fkJoinNodes.size(); j++) {
                // we don't need to adjust the generation strategy of the first 'FKJoin' node
                if (j == 0) {
                    fkJoinNodes.get(0).setFkJoinAdjustment(new FKJoinAdjustment(0, joinStatuses,
                            new ArrayList<FKJoinAdjustRule>(), fkJoinNodes.get(0).getProbability()));
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
        List<FKJoinAdjustRule> rules = new ArrayList<FKJoinAdjustRule>();
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

    private void computeNullProbability() {
        ComputeNullProbability computeNullProbability = new ComputeNullProbability(keyNullProbability, pkJoinInfo);
        try {
            tableNullProbability = computeNullProbability.computeConstraintChainNullProbabilityForEveryStatus();
        } catch (JOptimizerException e) {
            logger.info(e);
        }
        int allOnes = 0;
        for (Integer status : keyNullProbability.keySet()) {
            allOnes += status * 3;
        }
        for (Entry<Integer, ArrayList<long[]>> integerArrayListEntry : pkJoinInfo.entrySet()) {
            Collections.shuffle(integerArrayListEntry.getValue());
            double subPercentage = tableNullProbability.get(integerArrayListEntry.getKey() & allOnes);
            integerArrayListEntry.getValue().subList(0,
                    (int) (integerArrayListEntry.getValue().size() * subPercentage)).clear();
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
        if (keyNullProbability.size() != 0 && tableNullProbability == null) {
            computeNullProbability();
        }
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
                if (node.getType() == 2) {
                    FKJoin fkJoin = (FKJoin) node.getNode();
                    count += fkJoin.getFkJoinAdjustment().getRules().size();
                }
            }
        }
        return count;
    }
}

class Key implements Serializable {

    private static final long serialVersionUID = 1L;

    private String keyName = null;
    // 0: it's generated sequentially (uniqueness needs to be guaranteed)
    // 1: it's generated according to the its join statuses (it must be a foreign key)
    private int keyType;

    public Key(String keyName, int keyType) {
        super();
        this.keyName = keyName;
        this.keyType = keyType;
    }

    public Key(Key key) {
        super();
        this.keyName = key.keyName;
        this.keyType = key.keyType;
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

    public JoinStatusesSizePair(int joinStatuses, int size) {
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

    public int subAndGetSize(){
        return --size;
    }


    @Override
    public String toString() {
        return "\n\tJoinStatusesSizePair [joinStatuses=" + joinStatuses + ", size="
                + size + "]";
    }
}
