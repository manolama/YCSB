package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

public class RandomPrintableStringGenerator extends Generator<String> {
  public final static int DEFAULTSTRINGLENGTH = 8;

  private final int[] characterSet;
  
  private final int length;
  
  private String lastValue;
  
  public RandomPrintableStringGenerator() {
    this(DEFAULTSTRINGLENGTH);
  }
  
  public RandomPrintableStringGenerator(final int length) {
    this(length, IncrementingPrintableStringGenerator.printableBasicAlphaNumericASCIISet());
  }
  
  public RandomPrintableStringGenerator(final int length, final int[] characterSet) {
    this.length = length;
    this.characterSet = characterSet;
  }
  
  @Override
  public String nextValue() {
    // TODO - instead of returning a String that's then converted to a byte array 
    // then to a string for time series utilities, just return a byte buffer that
    // can be cast ONCE at the last. Sheesh.
    final StringBuilder buffer = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      buffer.append(Character.toChars(
          characterSet[Utils.random().nextInt(characterSet.length - 1)]));
    }
    lastValue = buffer.toString();
    return lastValue;
  }
  
  @Override
  public String lastValue() {
    return lastValue;
  }
  
}
