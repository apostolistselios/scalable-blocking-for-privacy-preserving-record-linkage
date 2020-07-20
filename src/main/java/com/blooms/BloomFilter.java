package com.blooms;
import java.security.MessageDigest;
import java.util.BitSet;

public class BloomFilter extends BitFilter<BitSet> implements Cloneable{
	protected String algorithm;
	protected int filterSize;
	protected static MessageDigest m=null;
	protected int numberOfHashFunctions;
	protected int digestLength = 0;
	protected int bytesPerHash = 0;


    protected long toLong(byte[] b, int offset, int sizeofword) {
        long value = 0;
        for (int i = 0; i < sizeofword; i++) {
            int shift = (sizeofword - 1 - i) * 8;
            value += (long)(b[i + offset] & 0x000000FF) << shift;
            //System.out.println(value);
        }
        return value;
    }


    public BloomFilter()
    {}

    public BloomFilter(String algorithm, int numberOfHashFunctions, int filterSize){
    	this.algorithm = algorithm;
    	if (m==null){
    	try{
    		m = MessageDigest.getInstance(algorithm);
    	}
    	catch (Exception e){
    		System.out.println(e);
    	}
    	}
    	this.numberOfHashFunctions = numberOfHashFunctions;
    	this.filterSize = filterSize;
    	this.filter = new BitSet(filterSize);
    	if(algorithm.compareTo("MD5")==0)
    		digestLength = 16;
    	/*
    	 * ��� �� ���� ������ ���� �� �������� ��� ��� ��������� hash functions, �� 1 �
    	 * 2. �� ������ ����� �� �������� �� ���� 4 bytes per hash.
    	 */
    	if(numberOfHashFunctions<5)
    		bytesPerHash = 4;
    	else
    	bytesPerHash = digestLength/numberOfHashFunctions;
    	//System.out.println(bytesPerHash);
    }

    public void add(String message){
    	byte[] digest;
    	int bitToSet;
    	long lbitToSet;
    	m.update(message.getBytes(),0,message.length());
    	digest = m.digest();
//    	System.out.println(algorithm+": "+new BigInteger(1,digest).toString(16));
    	m.reset();
    	for(int i=0;i<digestLength;i+=bytesPerHash){
    		lbitToSet = toLong(digest,i,bytesPerHash);
    		//System.out.println("number:"+lbitToSet+" To filter:"+(lbitToSet%this.filterSize));
    		lbitToSet=lbitToSet%this.filterSize;
    		bitToSet = (int)lbitToSet;
    		filter.set(bitToSet);
    		//System.out.println(filter.toString());
    	}
    }

    public boolean membershipTest(String message){
    	byte[] digest;
    	int bitToSet;
    	long lbitToSet;
    	m.update(message.getBytes(),0,message.length());
    	digest = m.digest();
    	m.reset();
    	for(int i=0;i<digestLength;i+=bytesPerHash){
    		lbitToSet = toLong(digest,i,bytesPerHash);
    		//System.out.println("number:"+lbitToSet+" To filter:"+(lbitToSet%this.filterSize));
    		lbitToSet=lbitToSet%this.filterSize;
    		bitToSet = (int)lbitToSet;
    		bitToSet%=this.filterSize;
    		if(!filter.get(bitToSet))
    			return(false);
    	}
    	return(true);
    }



    public String toString(){
    	return(filter.toString());
    }


    public BloomFilter clone(){
    	BloomFilter temp= new BloomFilter(this.algorithm, this.numberOfHashFunctions, this.filterSize);
    	temp.filter = (BitSet)this.filter.clone();
    	return(temp);
    }




}
