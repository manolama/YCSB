package com.yahoo.ycsb.generator;

import org.testng.annotations.Test;

public class TestRandomPrintableStringGenerator {

  @Test
  public void foobar() {
    final int[] characterSet = RandomPrintableStringGenerator.getFullASCIISet();
    for (int c : characterSet) {
      System.out.println("Char: [" + new String(Character.toChars(c)) + "] "
          + "cp [" + c + "]  type [" + Character.getType(c) + "] isAlpha [" + 
          Character.isAlphabetic(c) + "]");
    }
    System.out.println("Total set size: " + characterSet.length);
  }
}
