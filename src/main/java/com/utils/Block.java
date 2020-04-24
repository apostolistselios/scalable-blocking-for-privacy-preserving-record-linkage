package com.utils;

import java.io.Serializable;
import java.util.ArrayList;

public class Block implements Serializable {
	private static final long serialVersionUID = -3916723290247865993L;
	private String id;
	private ArrayList<BlockingAttribute> baList;
	
	public Block(String id, ArrayList<BlockingAttribute> baList) {
		this.id = id;
		this.baList = baList;
	}

	public String getId() {
		return id;
	}
	
	public void setBAList(ArrayList<BlockingAttribute> baList) {
		this.baList = baList;
	}
	
	public void addBlockingAttr(BlockingAttribute ba) {
		this.baList.add(ba);
	}
	
	public String toString() {
		return this.id + ": " + this.baList;
	}
}
