package com.yahoo.ycsb.generator;

import org.testng.annotations.Test;

public class TestOrderedPrintableStringGenerator {

  @Test
  public void blargedy() throws Exception {
    final int[] characterSet = new int[] { 65, 66, 67 };
    OrderedPrintableStringGenerator gen = new OrderedPrintableStringGenerator(2, characterSet);
    
    long start = System.nanoTime();
    for (int i = 0; i < 10; i++) {
      System.out.println(gen.nextString());
    }
    long end = System.nanoTime() - start;
    System.out.println("generated in " + ((double) end / (double)1000000) + " ms");
  }
}
