package com.utils;

public abstract class Timer {
    private static long t0;

    public static void start() {
        t0 = System.currentTimeMillis();
    }

    public static long stop() {
            return (System.currentTimeMillis() - t0) / 1000;
        }
}