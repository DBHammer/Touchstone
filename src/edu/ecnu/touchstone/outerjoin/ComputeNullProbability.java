package edu.ecnu.touchstone.outerjoin;

import com.joptimizer.exception.JOptimizerException;
import com.joptimizer.functions.ConvexMultivariateRealFunction;
import com.joptimizer.functions.LinearMultivariateRealFunction;
import com.joptimizer.functions.PDQuadraticMultivariateRealFunction;
import com.joptimizer.optimizers.JOptimizer;
import com.joptimizer.optimizers.OptimizationRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangqingshuai
 */
public class ComputeNullProbability {

    public static Map<Integer, double[]> computeTableAndFileNullProbability(Map<Integer, Double> nullProbability,
                                                                            Map<Integer, Long[]> mergedSizeInfo,
                                                                            int leftJoinTag) throws JOptimizerException {
        int leftJoinModTag = 3 * leftJoinTag;
        int leftJoinFalseTag = 2 * leftJoinModTag;

        Map<Integer, Double> taggedNullProbability = new HashMap<>();
        for (Map.Entry<Integer, Double> integerDoubleEntry : nullProbability.entrySet()) {
            if((integerDoubleEntry.getKey()& leftJoinTag)!=0){
                taggedNullProbability.put(integerDoubleEntry.getKey(),integerDoubleEntry.getValue());
            }
        }

        //compute sum size for every left join status
        Map<Integer, Long[]> taggedSizeInfo = new HashMap<>();
        for (Map.Entry<Integer, Long[]> sizeInfo : mergedSizeInfo.entrySet()) {
            int taggedStatus=sizeInfo.getKey()&leftJoinModTag;
            if(taggedStatus!=leftJoinFalseTag){
                if(taggedSizeInfo.containsKey(taggedStatus)){
                    Long[] size=taggedSizeInfo.get(taggedStatus);
                    size[0]+=sizeInfo.getValue()[0];
                    size[1]+=sizeInfo.getValue()[1];
                }else {
                    taggedSizeInfo.put(taggedStatus,sizeInfo.getValue());
                }
            }
        }

        if(taggedNullProbability.size()>1){
            Map<Integer, Long> taggedAllSizeInfo = new HashMap<>();
            for (Map.Entry<Integer, Long[]> integerEntry : taggedSizeInfo.entrySet()) {
                taggedAllSizeInfo.put(integerEntry.getKey(),integerEntry.getValue()[0]+integerEntry.getValue()[1]);
            }
            //compute null probability for every status
            taggedNullProbability = ComputeNullProbability.
                    computeConstraintChainNullProbabilityForEveryStatus(taggedNullProbability, taggedAllSizeInfo);
        }

        //compute null probability for table and file
        Map<Integer, double[]> tableAndFileNullProbability = new HashMap<>();
        for (Integer status : taggedNullProbability.keySet()) {
            double[] sol = ComputeNullProbability.computeTableAndFileNullProbability(taggedSizeInfo.get(status)[0],
                    taggedSizeInfo.get(status)[1], taggedNullProbability.get(status));
            tableAndFileNullProbability.put(status, sol);
        }
        return tableAndFileNullProbability;
    }


    /**
     * compute ConstraintChainNullProbabilityForEveryStatus
     *
     * @throws JOptimizerException the input matrix cannot compute the result
     */
    private static Map<Integer, Double> computeConstraintChainNullProbabilityForEveryStatus(
            Map<Integer, Double> eachKeyNullProbability, Map<Integer, Long> joinInfoSizes) throws JOptimizerException {
        //compute all status size
        int statusSize = (int) Math.pow(2, eachKeyNullProbability.size()) - 1;

        //compute a tag to decide what the status is
        int[] checkType = new int[eachKeyNullProbability.size()];
        checkType[0] = 1;
        for (int i = 1; i < checkType.length; i++) {
            checkType[i] = checkType[i - 1] * 2;
        }

        //init all status
        int[] joinStatusList = new int[statusSize];
        for (int i = 0; i < statusSize; i++) {
            int joinStatus = 0;
            int j = 0;
            for (Integer status : eachKeyNullProbability.keySet()) {
                if ((i & checkType[j]) == 0) {
                    joinStatus += status;
                } else {
                    joinStatus += 2 * status;
                }
                j++;
            }
            joinStatusList[i] = joinStatus;
        }

        //compute sum key size for each status
        Map<Integer, Long> joinInfoSumSizes = new HashMap<>(eachKeyNullProbability.size());
        for (Integer status : eachKeyNullProbability.keySet()) {
            joinInfoSumSizes.put(status, 0L);
        }
        for (Map.Entry<Integer, Long> joinInfoSumSize : joinInfoSumSizes.entrySet()) {
            long sum = 0;
            for (Map.Entry<Integer, Long> joinInfoSize : joinInfoSizes.entrySet()) {
                if ((joinInfoSumSize.getKey() & joinInfoSize.getKey()) == joinInfoSize.getKey()) {
                    sum += joinInfoSize.getValue();
                }
            }
            joinInfoSumSizes.put(joinInfoSumSize.getKey(), sum);
        }

        //init p,q,a,b
        double[][] p = new double[statusSize][statusSize];
        double[] q = new double[statusSize];
        double[][] a = new double[eachKeyNullProbability.size()][statusSize];
        double[] b = new double[eachKeyNullProbability.size()];

        //compute p,q,a
        for (int i = 0; i < statusSize; i++) {
            double pValue = 0;
            double qValue = 0;
            int j = 0;
            for (Map.Entry<Integer, Double> keyNullProbability : eachKeyNullProbability.entrySet()) {
                if ((i & checkType[j]) == 0) {
                    a[j][i] = joinInfoSizes.get(joinStatusList[i]);
                    double computePercentage = a[j][i] / joinInfoSumSizes.get(keyNullProbability.getKey());
                    pValue += computePercentage * computePercentage;
                    qValue += keyNullProbability.getValue() * computePercentage * computePercentage;
                }
                j++;
            }
            p[i][i] = pValue * 2;
            q[i] = qValue * -2;
        }

        //compute b
        int i = 0;
        for (Integer status : eachKeyNullProbability.keySet()) {
            b[i++] = eachKeyNullProbability.get(status) * joinInfoSumSizes.get(status);
        }

        //compute constraint chain null probability by JOptimiser
        double[] sol = computeConstraintChainNullProbability(p, q, a, b);

        //transform the result
        Map<Integer, Double> computeConstraintChainNullProbabilityForEveryStatus = new HashMap<>(sol.length);
        for (i = 0; i < sol.length; i++) {
            computeConstraintChainNullProbabilityForEveryStatus.put(joinStatusList[i], sol[i]);
        }
        return computeConstraintChainNullProbabilityForEveryStatus;
    }

    /**
     * Compute the null probability by JOptimiser
     *
     * @param p the value p in qp
     * @param q the value q in qp
     * @param a the value a in qp
     * @param b the value b in qp
     * @return the null probability
     * @throws JOptimizerException cannot compute a result
     */
    private static double[] computeConstraintChainNullProbability(double[][] p,
                                                                  double[] q,
                                                                  double[][] a,
                                                                  double[] b) throws JOptimizerException {
        //init optimization
        OptimizationRequest or = new OptimizationRequest();

        //set request function
        PDQuadraticMultivariateRealFunction objectiveFunction = new PDQuadraticMultivariateRealFunction(p, q, 0);
        or.setF0(objectiveFunction);

        //set inequalities function
        ConvexMultivariateRealFunction[] inequalities = new ConvexMultivariateRealFunction[2 * p.length];
        for (int i = 0; i < p.length; i++) {
            double[] temp = new double[p.length];
            temp[i] = -1;
            inequalities[i] = new LinearMultivariateRealFunction(temp, 0);
            temp[i] = 1;
            inequalities[i + p.length] = new LinearMultivariateRealFunction(temp, -1);
        }
        or.setFi(inequalities);

        //set Ax=b
        or.setA(a);
        or.setB(b);

        //optimization
        JOptimizer opt = new JOptimizer();
        opt.setOptimizationRequest(or);
        opt.optimize();
        return opt.getOptimizationResponse().getSolution();
    }

    /**
     * compute the table null probability and the file null probability
     *
     * @param tableKeySize         the table key size
     * @param fileKeySize          the file key size
     * @param totalNullProbability the null probability of all
     * @return the table null probability and the file null probability
     */
    private static double[] computeTableAndFileNullProbability(long tableKeySize,
                                                               long fileKeySize,
                                                               double totalNullProbability) throws JOptimizerException {
        //init optimization
        OptimizationRequest or = new OptimizationRequest();

        //set request function
        LinearMultivariateRealFunction objectiveFunction =
                new LinearMultivariateRealFunction(new double[]{1., 0.}, 0);
        or.setF0(objectiveFunction);

        //set inequalities function
        ConvexMultivariateRealFunction[] inequalities = new ConvexMultivariateRealFunction[4];
        inequalities[0] = new LinearMultivariateRealFunction(new double[]{-1., 0.}, 0);
        inequalities[1] = new LinearMultivariateRealFunction(new double[]{1., 0.}, 1);
        inequalities[2] = new LinearMultivariateRealFunction(new double[]{0., -1.}, 0);
        inequalities[3] = new LinearMultivariateRealFunction(new double[]{0., 1.}, 1);
        or.setFi(inequalities);

        //set Ax=b
        or.setA(new double[][]{{tableKeySize, fileKeySize}});
        or.setB(new double[]{totalNullProbability * (tableKeySize + fileKeySize)});

        //optimization
        JOptimizer opt = new JOptimizer();
        opt.setOptimizationRequest(or);
        opt.optimize();
        return opt.getOptimizationResponse().getSolution();
    }
}

