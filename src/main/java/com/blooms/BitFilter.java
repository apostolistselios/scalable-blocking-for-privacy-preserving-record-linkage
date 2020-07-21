package com.blooms;

import java.util.BitSet;

public abstract class BitFilter<T> extends Filter<Integer> {
	protected BitSet filter;

	
	public BitSet getFilter() {
		return filter;
	}


	public BitFilter<BitSet> clone(){
		return(this.clone());
	}
	
	
	float compareFilters(BitFilter<BitSet> otherFilter){
		BitFilter<BitSet> copyOfOtherFilter,copyOfThisFilter;
		float diceCo = 0f;
		copyOfOtherFilter = otherFilter.clone();
		copyOfThisFilter = this.clone();
		copyOfOtherFilter.getFilter().and(copyOfThisFilter.getFilter());

		diceCo = 2*(float)copyOfOtherFilter.getFilter().cardinality()/(otherFilter.getFilter().cardinality()
		+ this.getFilter().cardinality());

		return diceCo;
	}
}
