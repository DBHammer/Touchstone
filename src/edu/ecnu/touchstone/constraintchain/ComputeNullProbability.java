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
    public ComputeNullProbability(ArrayList<ComputeNode> computeNodes) {
        int statusSize = (int) Math.pow(2, computeNodes.size()) - 1;
        int[] checkType = new int[computeNodes.size()];
        checkType[0] = 1;
        for (int i = 1; i < checkType.length; i++) {
            checkType[i] = checkType[i - 1] * 2;
        }
        P = new double[statusSize][statusSize];
        q = new double[statusSize];
        matrixA = new double[computeNodes.size()][statusSize];
        b = new double[computeNodes.size()];
        joinStatusList = new int[statusSize];
        for (int i = 0; i < statusSize; i++) {
            double dataPercentage = 1;
            int joinStatus = 0;
            for (int j = 0; j < computeNodes.size(); j++) {
                if ((i & checkType[j]) == 0) {
                    dataPercentage *= computeNodes.get(j).getDataPercentage();
                    joinStatus += computeNodes.get(j).getStatus();
                } else {
                    dataPercentage *= 1 - computeNodes.get(j).getDataPercentage();
                    joinStatus += 2 * computeNodes.get(j).getStatus();
                }
            }
            joinStatusList[i] = joinStatus;

            double pValue = 0;
            double qValue = 0;
            for (int j = 0; j < computeNodes.size(); j++) {
                if ((i & checkType[j]) == 0) {
                    matrixA[j][i] = dataPercentage;
                    double computePercentage = dataPercentage / computeNodes.get(j).getDataPercentage();
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
    private Map<Integer,Double> computeConstraintChainNullProbabilityForEveryStatus() throws JOptimizerException {
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
        Map<Integer,Double>computeConstraintChainNullProbabilityForEveryStatus=new HashMap<>(sol.length);
        for (int i = 0; i < sol.length; i++) {
            computeConstraintChainNullProbabilityForEveryStatus.put(joinStatusList[i],sol[i]);
        }
        return computeConstraintChainNullProbabilityForEveryStatus;
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("running examples/lib/log4j.properties");
        ArrayList<ComputeNode> computeNodes = new ArrayList<>();
        computeNodes.add(new ComputeNode(1, 0.20, 0.76));
        computeNodes.add(new ComputeNode(4, 0.4, 0.40));
        computeNodes.add(new ComputeNode(16, 0.75, 0.50));
        computeNodes.add(new ComputeNode(64, 0.73, 0.34));
        ComputeNullProbability computeNullProbability = new ComputeNullProbability(computeNodes);
        System.out.println(computeNullProbability);
        try {
            computeNullProbability.computeConstraintChainNullProbabilityForEveryStatus();
        } catch (JOptimizerException e) {
            e.printStackTrace();
        }
        System.out.println(computeNullProbability);
    }
}

class ComputeNode {
    private int status;
    private double nullProbability;
    private double dataPercentage;

    int getStatus() {
        return status;
    }

    double getNullProbability() {
        return nullProbability;
    }

    double getDataPercentage() {
        return dataPercentage;
    }

    ComputeNode(int status, double dataPercentage, double nullProbability) {
        this.status = status;
        this.nullProbability = nullProbability;
        this.dataPercentage = dataPercentage;
    }
}
