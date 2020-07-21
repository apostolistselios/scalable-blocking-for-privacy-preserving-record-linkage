package com.blooms;

import java.util.LinkedHashMap;


public abstract class Filter<T> implements Cloneable{
	abstract public Filter<T> clone();

	public float compareFilters(Filter<T> otherFilter){
		return -1.f;
	}


	public void setAll(LinkedHashMap<Integer, Float> referenceIds){
		
	}

}

