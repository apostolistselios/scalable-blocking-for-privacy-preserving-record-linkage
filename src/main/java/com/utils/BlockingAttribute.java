package com.utils;

import java.io.Serializable;

public class BlockingAttribute implements Serializable, Comparable<BlockingAttribute>{
	private static final long serialVersionUID = 8825524692657181465L;
	private String classID;
	private String recordID;
	private int score;
	
	public BlockingAttribute(String classID, int score) {
		this.classID = classID;
		this.score = score;
	}

	public String getRecordID() {
		return recordID;
	}

	public void setRecordID(String id) {
		this.recordID = id;
	}
	
	public String getClassID() {
		return classID;
	}

	public void setClassID(String classID) {
		this.classID = classID;
	}

	public int getScore() {
		return score;
	}
	
	public void setScore(int score) {
		this.score = score;
	}
	
	@Override
	public String toString() {
		return "BA(" + String.join(",", this.classID, this.recordID, String.valueOf(this.score)) + ")";
	}

	@Override
	public int compareTo(BlockingAttribute other) {
		int otherScore = other.getScore();
		return this.score - otherScore;	
	}
}
