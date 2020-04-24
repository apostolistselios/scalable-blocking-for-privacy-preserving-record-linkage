package com.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class Block implements Serializable {
	private static final long serialVersionUID = -3916723290247865993L;
	private String id;
	private ArrayList<BlockingAttribute> baList;
	private int rank;
	
	public Block(String id, ArrayList<BlockingAttribute> baList) {
		this.id = id;
		this.baList = baList;
	}

	public String getId() {
		return id;
	}
	
	public ArrayList<BlockingAttribute> getBAList() {
		return this.baList;
	}
	
	public void setBAList(ArrayList<BlockingAttribute> baList) {
		this.baList = baList;
	}
	
	public void addBlockingAttr(BlockingAttribute ba) {
		this.baList.add(ba);
	}
	
	public int getRank() {
		return this.rank;
	}
	
	public void calculateRank() {
		int rank = 0;
		for (BlockingAttribute ba : this.baList) {
			rank += ba.getScore();
		}
		this.rank = rank;
	}
	
	public String toString() {
		return "[BLOCK: " + this.id + " - Rank: " + this.rank + " - " + this.baList + "]";
	}
}
