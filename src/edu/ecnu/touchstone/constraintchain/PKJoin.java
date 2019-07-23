package edu.ecnu.touchstone.constraintchain;

import java.io.Serializable;
import java.util.Arrays;

// pattern: [1, pk1#pk2 ..., num1, num2, ...]
// num1 is the identifier that can join, num2 is the identifier that can not join
// for every primary key, the identifier (num1 and num2) must be unique
// num1 and num2 can appear multiple pairs (a primary key may be joined with multiple associated foreign keys)
// the primary key can be multiple attributes (support mixed reference)
public class PKJoin implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String[] primaryKeys = null;

	public double[] getLeftOuterJoinNullProbability() {
		return leftOuterJoinNullProbability;
	}

	private double[] leftOuterJoinNullProbability;
	
	// aligned in sequence
	private int[] canJoinNum = null;
	private int[] cantJoinNum = null;
	
	// to avoid the string manipulation in data generation
	private String pkStr = null;

	public PKJoin(String[] primaryKeys, int[] canJoinNum, int[] cantJoinNum, double[] leftOuterJoinNullProbability) {
		super();
		this.primaryKeys = primaryKeys;
		this.canJoinNum = canJoinNum;
		this.cantJoinNum = cantJoinNum;
		this.leftOuterJoinNullProbability=leftOuterJoinNullProbability;
		pkStr = Arrays.toString(this.primaryKeys);
	}

	public PKJoin(PKJoin pkJoin) {
		super();
		this.primaryKeys = Arrays.copyOf(pkJoin.primaryKeys, pkJoin.primaryKeys.length);
		this.canJoinNum = Arrays.copyOf(pkJoin.canJoinNum, pkJoin.canJoinNum.length);
		this.cantJoinNum = Arrays.copyOf(pkJoin.cantJoinNum, pkJoin.cantJoinNum.length);
		if(pkJoin.leftOuterJoinNullProbability!=null){
			this.leftOuterJoinNullProbability= Arrays.copyOf(pkJoin.leftOuterJoinNullProbability,
					pkJoin.leftOuterJoinNullProbability.length);
		}
		this.pkStr = pkJoin.pkStr;
	}

	public String[] getPrimaryKeys() {
		return primaryKeys;
	}

	public int[] getCanJoinNum() {
		return canJoinNum;
	}

	public int[] getCantJoinNum() {
		return cantJoinNum;
	}

	public String getPkStr() {
		return pkStr;
	}

	@Override
	public String toString() {
		return "\n\tPKJoin [primaryKeys=" + Arrays.toString(primaryKeys) + ", canJoinNum=" + Arrays.toString(canJoinNum)
				+ ", cantJoinNum=" + Arrays.toString(cantJoinNum) + ", pkStr=" + pkStr + "]";
	}
}