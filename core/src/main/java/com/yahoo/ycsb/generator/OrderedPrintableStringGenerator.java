package com.yahoo.ycsb.generator;

public class OrderedPrintableStringGenerator extends Generator<String> {

  private final int[] characterSet;
  
  private int[] indexes; 
  
  private final int length;
  
  private String lastValue;
  
  private boolean exceptionOnRollover;
  
  public OrderedPrintableStringGenerator() {
    this(RandomPrintableStringGenerator.DEFAULTSTRINGLENGTH,
        RandomPrintableStringGenerator.getAlphaASCIISet());
  }
  
  public OrderedPrintableStringGenerator(final int length) {
    this(length,
        RandomPrintableStringGenerator.getAlphaASCIISet());
  }
  
  public OrderedPrintableStringGenerator(final int length, final int[] characterSet) {
    this.length = length;
    this.characterSet = characterSet;
    indexes = new int[length];
  }
  
  @Override
  public String nextValue() {
    
    final StringBuilder buffer = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      buffer.append(Character.toChars(characterSet[indexes[i]]));
    }
    lastValue = buffer.toString();
    
    // increment the indices;
    for (int i = length - 1; i >= 0; --i) {
      if (indexes[i] >= characterSet.length - 1) {
        indexes[i] = 0;
        if (i == 0 && exceptionOnRollover) {
          throw new RuntimeException("The generator has rolled over to the beginning");
        }
      } else {
        ++indexes[i];
        break;
      }
    }
    
    return lastValue;
  }

  @Override
  public String lastValue() {
    return lastValue;
  }

}
