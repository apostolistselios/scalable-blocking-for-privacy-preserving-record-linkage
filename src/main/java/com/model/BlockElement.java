package com.model;

import java.io.Serializable;

public class BlockElement implements Serializable, Comparable<BlockElement>{

    private String recordID;
    private int score;

    public BlockElement(String recordID, int score)  {
        this.recordID = recordID;
        this.score = score;
    }


    public String getRecordID() {
        return recordID;
    }

    public void setRecordID(String recordID) {
        this.recordID = recordID;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "BE(" + String.join(",", this.recordID, String.valueOf(this.score)) + ")";
    }

    @Override
    public int compareTo(BlockElement other) {
        int otherScore = other.getScore();
        return this.score - otherScore;
    }
}
