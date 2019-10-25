package org.wso2.das.integration.tests.util;

/**
 * Used to perform backoff retrying.
 */
public class BackoffRetryCounter {
    private final long[] timeIntervals;
    private int intervalIndex = 0;

    public BackoffRetryCounter() {
        timeIntervals = new long[]{500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500};
    }

    public BackoffRetryCounter(long ...intervals) {
        long[] longArray = new long[intervals.length];
        int index = 0;
        while (index < intervals.length) {
            longArray[index] = intervals[index];
            index++;
        }
        timeIntervals = longArray;
    }

    public synchronized void reset() {
        intervalIndex = 0;
    }

    public synchronized void increment() {
        if (intervalIndex < timeIntervals.length - 2) {
            intervalIndex++;
        }
    }

    public long getTimeInterval() {
        return timeIntervals[intervalIndex];
    }

    public boolean shouldStop() {
        return intervalIndex == timeIntervals.length - 1;
    }
}
