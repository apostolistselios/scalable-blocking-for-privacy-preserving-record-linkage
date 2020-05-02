package com.utils;

import java.util.List;

public class BinarySearch {
    public static int binarySearch(List<String> arr, int low, int high, String x){

        if (high >= low) {
            int mid = low +(int) Math.ceil((double) (high - low ) / (double) 2 ) ;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).substring(0,1).equals(x.substring(0,1)) )
                return mid;


            // If element is smaller than mid, then
            // it can only be present in right subarray
            if ( x.substring(0,1).compareTo(arr.get(mid).substring(0,1)) > 0  ) {
                return binarySearch(arr, mid + 1, high, x);
            }
            else if (mid == arr.size()-1) {
                return mid;
            }

            // Else the element can only be present
            // in left subarray

            return binarySearch(arr, low, mid-1, x);
        }

        // We reach here when element is not present
        // in array
        if (high < 0) {return high +1  ;}
        else if (high >3) {return high-1 ; }
        else  return high  ;
    }
}
