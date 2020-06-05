package com.utils;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.sun.tools.javac.jvm.ByteCodes.swap;

public abstract class DurstenfeldShuffle {
    public static void shuffle(List <String> arr){
        // Creating a object for Random class
        Random r = new Random();

        // Start from the last element and swap one by one. We don't
        // need to run for the first element that's why i > 0
        for (int i = Conf.RS_SIZE - 1; i > 0; --i) {
            swap(arr, i, r.nextInt(i+1));
        }
    }

    private static void swap(List<String> data, int i, int j) {
        String tmp = data.get(i);
        data.set(i, data.get(j));
        data.set(j, tmp);
    }
}
