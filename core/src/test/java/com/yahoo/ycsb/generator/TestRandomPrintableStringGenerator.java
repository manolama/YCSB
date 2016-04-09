package com.yahoo.ycsb.generator;

import org.testng.annotations.Test;

import com.yahoo.ycsb.Utils;

public class TestRandomPrintableStringGenerator {

  @Test
  public void foobar() {
    final int[] characterSet = IncrementingPrintableStringGenerator.printableBasicAlphaASCIISet();
    for (int c : characterSet) {
      System.out.println("Char: [" + new String(Character.toChars(c)) + "] "
          + "cp [" + c + "]  type [" + Character.getType(c) + "] isAlpha [" + 
          Character.isAlphabetic(c) + "]");
    }
    System.out.println("Total set size: " + characterSet.length);
  }
}
