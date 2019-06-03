package edu.ecnu.touchstone.constraintchain;

public class ComputeNode {
    private int status;
    private double nullProbability;
    private double dataPercentage;

    public int getStatus() {
        return status;
    }

    public double getNullProbability() {
        return nullProbability;
    }

    double getDataPercentage() {
        return dataPercentage;
    }

    public ComputeNode(int status, double dataPercentage, double nullProbability) {
        this.status = status;
        this.nullProbability = nullProbability;
        this.dataPercentage = dataPercentage;
    }
}
