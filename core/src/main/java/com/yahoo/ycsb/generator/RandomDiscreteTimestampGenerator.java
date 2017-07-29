package com.yahoo.ycsb.generator;

import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.Utils;

public class RandomDiscreteTimestampGenerator extends UnixEpochTimestampGenerator {

  public static final int MAX_INTERVALS = 16777216;
  
  private final int intervals;
  // can't be primitives due to the generic params on the sort function :(
  private final Integer[] offsets;
  private int offsetIndex;
  
  public RandomDiscreteTimestampGenerator(final long interval, final TimeUnit timeUnits, final int intervals) {
    super(interval, timeUnits);
    this.intervals = intervals;
    offsets = new Integer[intervals];
    setup();
  }
  
  public RandomDiscreteTimestampGenerator(final long interval, final TimeUnit timeUnits,
                                     final long startTimestamp, final int intervals) {
    super(interval, timeUnits, startTimestamp);
    this.intervals = intervals;
    offsets = new Integer[intervals];
    setup();
  }
  
  void setup() {
    if (intervals > MAX_INTERVALS) {
      throw new IllegalArgumentException("Too many intervals for the in-memory "
          + "array. The limit is " + MAX_INTERVALS + ".");
    }
    offsetIndex = 0;
    for (int i = 0; i < intervals; i++) {
      offsets[i] = i;
    }
    Utils.shuffleArray(offsets);
  }
  
  @Override
  public Long nextValue() {
    if (offsetIndex >= offsets.length) {
      throw new IllegalStateException("Reached the end of the random timestamp "
          + "intervals: " + offsetIndex);
    }
    lastTimestamp = currentTimestamp;
    currentTimestamp = startTimestamp + (offsets[offsetIndex++] * getOffset(1));
    return currentTimestamp;
  }
}
