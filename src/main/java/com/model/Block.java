package com.model;

import java.util.ArrayList;

public class Block  {

	private String id;
	private ArrayList<BlockElement> baList;
	private int rank;
	
	public Block(String id, ArrayList<BlockElement> baList) {
		this.id = id;
		this.baList = baList;
	}

	public String getId() {
		return id;
	}
	
	public ArrayList<BlockElement> getBAList() {
		return this.baList;
	}
	
	public void setBAList(ArrayList<BlockElement> baList) {
		this.baList = baList;
	}
	
	public void addBlockingAttr(BlockElement ba) {
		this.baList.add(ba);
	}
	
	public int getRank() {
		return this.rank;
	}
	
	public void calculateRank() {
		int rank = 0;
		for (BlockElement ba : this.baList) {
			rank += ba.getScore();
		}
		this.rank = rank;
	}
	
	public String toString() {
		return "[BLOCK: " + this.id + " - Rank: " + this.rank + " - " + this.baList + "]";
	}
}
