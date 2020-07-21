package com.utils;

import com.blooms.BloomFilter;

import java.util.ArrayList;
import java.util.List;

public class BloomAlgorithms {
    public static List<String> ngrams(int n, String str) {
        List<String> ngrams = new ArrayList<String>();
        for (int i = 0; i < str.length() - n + 1; i++)
            // Add the substring or size n
            ngrams.add(str.substring(i, i + n));
        // In each iteration, the window moves one step forward
        // Hence, each n-gram is added to the list

        return ngrams;
    }

    public static String pad(String text, int ngsize){
		String paddedText=text.trim(), leading = "#", trailing="%";
		int i=0;
		for (i=0;i<ngsize-1;i++)
			paddedText = leading+paddedText+trailing;
		return paddedText;
	}

	public static BloomFilter string2Bloom(String data){
		BloomFilter f = new BloomFilter("MD5",Conf.HASH_FUNCTIONS,Conf.bloomFilterSize*Conf.M_N_RATIO);
		String padded = "";
		if(data!=null && data.length() > 1 ){
			padded = pad(data,Conf.nGramSize);
			for(int k=0; k<padded.length()-Conf.nGramSize+1;k++){
				f.add(padded.substring(k,Conf.nGramSize+k));
			}
		}
		return f;
	}
}
