package com.utils;

import java.util.List;

public class BinarySearch {
    public static int binarySearch(List<String> arr, int low, int high, String x, int num_of_search_chars){

        if (high >= low) {
            int mid = (int) (low +(int) (double) (high - low ) / (double) 2);

            // If the element is present at the
            // middle itself
            if (arr.get(mid).substring(0,num_of_search_chars).equals(x.substring(0,num_of_search_chars)) )
                return mid;


            // If element is smaller than mid, then
            // it can only be present in right subarray
            if ( x.substring(0,num_of_search_chars).compareTo(arr.get(mid).substring(0,num_of_search_chars)) > 0  ) {
                return binarySearch(arr, mid + 1, high, x, num_of_search_chars);
            }
//            else if (mid == arr.size()-1) {
//                return mid;
//            }

            // Else the element can only be present
            // in left subarray

            return binarySearch(arr, low, mid-1, x, num_of_search_chars);
        }

        // We reach here when element is not present
        // in array
        if (high < 0) {return high +1  ;}
        else if (high > Conf.RS_SIZE -1 ) {return high-1 ; }
        else  return high  ;
    }
}
