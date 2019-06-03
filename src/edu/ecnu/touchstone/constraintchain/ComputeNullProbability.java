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
    public ComputeNullProbability(ArrayList<ComputeNode> computeNodes,
                                  Map<Integer, ArrayList<long[]>> fkJoinInfo) {
        int statusSize = (int) Math.pow(2, computeNodes.size()) - 1;
        int[] checkType = new int[computeNodes.size()];
        checkType[0] = 1;
        for (int i = 1; i < checkType.length; i++) {
            checkType[i] = checkType[i - 1] * 2;
        }

        joinStatusList = new int[statusSize];

        //初始化当前的join组信息
        Map<Integer,Double>fkJoinPercentage=new HashMap<>();
        for (int i = 0; i < statusSize; i++) {
            int joinStatus = 0;
            for (int j = 0; j < computeNodes.size(); j++) {
                if ((i & checkType[j]) == 0) {
                    joinStatus += computeNodes.get(j).getStatus();
                } else {
                    joinStatus += 2 * computeNodes.get(j).getStatus();
                }
            }
            fkJoinPercentage.put(joinStatus,0D);
            joinStatusList[i] = joinStatus;
        }

        int allOnes=3*joinStatusList[0];
        long fkJoinInfoSize=0;
        for (ArrayList<long[]> value : fkJoinInfo.values()) {
            fkJoinInfoSize+=value.size();
        }
        for (Map.Entry<Integer, ArrayList<long[]>> integerArrayListEntry : fkJoinInfo.entrySet()) {
            int status=integerArrayListEntry.getKey()&allOnes;
            double percentage=(double)integerArrayListEntry.getValue().size()/fkJoinInfoSize;
            fkJoinPercentage.put(status, percentage+fkJoinPercentage.get(status));
        }

        P = new double[statusSize][statusSize];
        q = new double[statusSize];
        matrixA = new double[computeNodes.size()][statusSize];
        b = new double[computeNodes.size()];
        for (int i = 0; i < statusSize; i++) {
            double pValue = 0;
            double qValue = 0;
            for (int j = 0; j < computeNodes.size(); j++) {
                if ((i & checkType[j]) == 0) {
                    matrixA[j][i] = fkJoinPercentage.get(joinStatusList[i]);
                    double computePercentage = matrixA[j][i] / computeNodes.get(j).getDataPercentage();
                    pValue += computePercentage * computePercentage;
                    qValue += computeNodes.get(j).getNullProbability() * computePercentage * computePercentage;
                } else {
                    matrixA[j][i] = 0;
                }
            }
            P[i][i] = pValue * 2;
            q[i] = qValue * -2;
        }
        for (int i = 0; i < computeNodes.size(); i++) {
            b[i] = computeNodes.get(i).getNullProbability() * computeNodes.get(i).getDataPercentage();
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

    public static void main(String[] args) {
        PropertyConfigurator.configure("running examples/lib/log4j.properties");
        ArrayList<ComputeNode> computeNodes = new ArrayList<>();
        computeNodes.add(new ComputeNode(1, 0.20, 0.76));
        computeNodes.add(new ComputeNode(4, 0.4, 0.40));
//        computeNodes.add(new ComputeNode(16, 0.75, 0.50));
//        computeNodes.add(new ComputeNode(64, 0.73, 0.34));
        ComputeNullProbability computeNullProbability = new ComputeNullProbability(computeNodes,null);
        System.out.println(computeNullProbability);
        try {
            computeNullProbability.computeConstraintChainNullProbabilityForEveryStatus();
        } catch (JOptimizerException e) {
            e.printStackTrace();
        }
        System.out.println(computeNullProbability);
    }
}

