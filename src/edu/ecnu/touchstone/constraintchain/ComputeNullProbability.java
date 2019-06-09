package edu.ecnu.touchstone.constraintchain;

import com.joptimizer.exception.JOptimizerException;
import com.joptimizer.functions.ConvexMultivariateRealFunction;
import com.joptimizer.functions.LinearMultivariateRealFunction;
import com.joptimizer.functions.PDQuadraticMultivariateRealFunction;
import com.joptimizer.optimizers.JOptimizer;
import com.joptimizer.optimizers.OptimizationRequest;
import org.apache.log4j.PropertyConfigurator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ComputeNullProbability {
    private int[] joinStatusList;
    private double[][] P;
    private double[] q;
    private double[][] matrixA;
    private double[] b;

    /**
     * acquire a dataPercentage and constraintChainNullProbability
     */
    public ComputeNullProbability(Map<Integer,Double> keyNullProbability,
                                  Map<Integer, ArrayList<long[]>> fkJoinInfo) {
        assert fkJoinInfo!=null;
        int statusSize = (int) Math.pow(2, keyNullProbability.size()) - 1;

        int[] checkType = new int[keyNullProbability.size()];
        checkType[0] = 1;
        for (int i = 1; i < checkType.length; i++) {
            checkType[i] = checkType[i - 1] * 2;
        }

        //初始化当前的join组信息
        joinStatusList = new int[statusSize];
        Map<Integer,Double>joinTableDataPercentage=new HashMap<>();
        for (int i = 0; i < statusSize; i++) {
            int joinStatus = 0;
            int j=0;
            for (Integer status : keyNullProbability.keySet()) {
                if ((i & checkType[j]) == 0) {
                    joinStatus += status;
                } else {
                    joinStatus += 2 * status;
                }
                j++;
            }
            joinTableDataPercentage.put(joinStatus,0D);
            joinStatusList[i] = joinStatus;
        }
        joinTableDataPercentage.put(2*joinStatusList[0],0D);

        int allOnes=3*joinStatusList[0];
        long fkJoinInfoSize=0;
        for (ArrayList<long[]> value : fkJoinInfo.values()) {
            fkJoinInfoSize+=value.size();
        }
        for (Map.Entry<Integer, ArrayList<long[]>> integerArrayListEntry : fkJoinInfo.entrySet()) {
            int status=integerArrayListEntry.getKey()&allOnes;
            double percentage=(double)integerArrayListEntry.getValue().size()/fkJoinInfoSize;
            joinTableDataPercentage.put(status, percentage+joinTableDataPercentage.get(status));
        }

        //计算sum dataPercentage
        Map<Integer,Double> joinTableSumDataPercentage=new HashMap<>();
        for (Integer status : keyNullProbability.keySet()) {
            joinTableSumDataPercentage.put(status,0D);
        }
        for (Map.Entry<Integer, Double> statusDataPercentage : joinTableDataPercentage.entrySet()) {
            for (Integer status : joinTableSumDataPercentage.keySet()) {
                if((statusDataPercentage.getKey()&status)==status){
                    joinTableSumDataPercentage.put(status,joinTableSumDataPercentage.get(status)
                            +statusDataPercentage.getValue());
                }
            }
        }

        P = new double[statusSize][statusSize];
        q = new double[statusSize];
        matrixA = new double[keyNullProbability.size()][statusSize];
        b = new double[keyNullProbability.size()];
        for (int i = 0; i < statusSize; i++) {
            double pValue = 0;
            double qValue = 0;
            int j=0;
            for (Integer status : keyNullProbability.keySet()) {
                if ((i & checkType[j]) == 0) {
                    matrixA[j][i] = joinTableDataPercentage.get(joinStatusList[i]);
                    double computePercentage = matrixA[j][i] / joinTableSumDataPercentage.get(status);
                    pValue += computePercentage * computePercentage;
                    qValue += keyNullProbability.get(status) * computePercentage * computePercentage;
                } else {
                    matrixA[j][i] = 0;
                }
                j++;
            }
            P[i][i] = pValue * 2;
            q[i] = qValue * -2;
        }
        int i=0;
        for (Integer status : keyNullProbability.keySet()) {
            b[i++] = keyNullProbability.get(status) * joinTableSumDataPercentage.get(status);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("the computed values are\n");
        result.append("P:");
        for (int i = 0; i < P.length; i++) {
            result.append(P[i][i]).append(",");
        }
        result.append("\nq:");
        for (double v : q) {
            result.append(v).append(",");
        }
        result.append("\nmatrixA:\n");
        for (double[] doubles : matrixA) {
            for (Double aDouble : doubles) {
                result.append(aDouble).append(",");
            }
            result.append("\n");
        }
        result.append("b:");
        for (double v : b) {
            result.append(v).append(",");
        }

        return result.toString();
    }

    /**
     * compute ConstraintChainNullProbabilityForEveryStatus
     *
     * @throws JOptimizerException the input matrix cannot compute the result
     */
    public Map<Integer, Double> computeConstraintChainNullProbabilityForEveryStatus() throws JOptimizerException {
        PDQuadraticMultivariateRealFunction objectiveFunction = new PDQuadraticMultivariateRealFunction(P, q, 0);
        //optimization problem
        OptimizationRequest or = new OptimizationRequest();
        or.setF0(objectiveFunction);
        or.setA(matrixA);
        or.setB(b);
        JOptimizer opt = new JOptimizer();

        //inequalities limit
        ConvexMultivariateRealFunction[] inequalities = new ConvexMultivariateRealFunction[2 * P.length];
        for (int i = 0; i < P.length; i++) {
            double[] temp = new double[P.length];
            temp[i] = -1;
            inequalities[i] = new LinearMultivariateRealFunction(temp, 0);
            temp[i] = 1;
            inequalities[i + P.length] = new LinearMultivariateRealFunction(temp, -1);
        }
        or.setFi(inequalities);
        opt.setOptimizationRequest(or);
        opt.optimize();
        double[] sol = opt.getOptimizationResponse().getSolution();
        Map<Integer, Double> computeConstraintChainNullProbabilityForEveryStatus = new HashMap<>(sol.length);
        for (int i = 0; i < sol.length; i++) {
            computeConstraintChainNullProbabilityForEveryStatus.put(joinStatusList[i], sol[i]);
        }
        computeConstraintChainNullProbabilityForEveryStatus.put(2 * joinStatusList[0], 0D);
        return computeConstraintChainNullProbabilityForEveryStatus;
    }
}

