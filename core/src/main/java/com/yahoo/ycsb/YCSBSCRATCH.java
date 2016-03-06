package com.yahoo.ycsb;
import java.util.ArrayList;
import java.util.List;

import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.RandomPrintableStringGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.UniformGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

public class YCSBSCRATCH {

  public static void main(String[] args) {
    randomPrintableStringGenerator();
    //stringByteIterator();
    //uniformGenerator();
    //scrambledZipfianGenerator();
    //hotspotIntegerGenerator();
    //exponentialGenerator();
    //counterGenerator();
    //uniformIntegerGenerator();
    //discreteGenerator();
    //constantIntegerGenerator();
  }
  
  static void testCharSets() {
    //  final int[] characterSet = RandomPrintableStringGenerator.generateCharacterSet(
    //  127, null, false, RandomPrintableStringGenerator.ALLBUTCONTROL);
    final int[] characterSet = RandomPrintableStringGenerator.getFullASCIISet();
    for (int c : characterSet) {
    System.out.println("Char: [" + new String(Character.toChars(c)) + "] "
        + "cp [" + c + "]  type [" + Character.getType(c) + "] isAlpha [" + 
        Character.isAlphabetic(c) + "]");
    }
    System.out.println("Total set size: " + characterSet.length);
  }
  
  static void randomPrintableStringGenerator() {
    RandomPrintableStringGenerator gen = //new RandomPrintableStringGenerator();
        new RandomPrintableStringGenerator(256, RandomPrintableStringGenerator.getFullPlaneZeroSet());
    long st = System.nanoTime();
    for (int i = 0; i < 1000000; i++) {
      if (i < 40) {
        System.out.println(gen.nextValue());
      } else {
        gen.nextValue();
      }
      //System.out.println(gen.nextValue());
    }
    double end = ((double)System.nanoTime() - st) / (double)1000000;
    System.out.println("Write 1M in " + end + "ms");
  }
  
  static void stringByteIterator() {
    StringByteIterator it = new StringByteIterator("hello!");
    for (int i = 0; i < 40; i++) {
      if (!it.hasNext()) {
        System.out.println("No more... :(");
        break;
      }
      System.out.println(it.bytesLeft());
    }
  }
  
  static void uniformGenerator() {
    List<String> vals = new ArrayList<String>(5);
    vals.add("A");
    vals.add("B");
    vals.add("C");
    vals.add("D");
    vals.add("E");
    UniformGenerator gen = new UniformGenerator(vals);
    for (int i = 0; i < 40; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  static void scrambledZipfianGenerator() {
    ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(1000);
    for (int i = 0; i < 40; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  static void hotspotIntegerGenerator() {
    HotspotIntegerGenerator gen = new HotspotIntegerGenerator(0, 42, .1, .9);
    for (int i = 0; i < 40; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  static void exponentialGenerator() {
    ExponentialGenerator gen = new ExponentialGenerator(.5);
    for (int i = 0; i < 40; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  static void counterGenerator() {
    CounterGenerator gen = new CounterGenerator(1);
    for (int i = 0; i < 4; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  static void constantIntegerGenerator() {
    ConstantIntegerGenerator gen = new ConstantIntegerGenerator(42);
    for (int i = 0; i < 4; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  static void uniformIntegerGenerator() {
    UniformIntegerGenerator gen = new UniformIntegerGenerator(0, 42);
    for (int i = 0; i < 4; i++) {
      System.out.println(gen.nextValue());
    }
  }
  
  /** This guy you have to add values first */
  static void discreteGenerator() {
    DiscreteGenerator gen = new DiscreteGenerator();
    gen.addValue(42, "woot");
    gen.addValue(24, "bar");
    gen.addValue(1, "tool");
    gen.addValue(2, "foo");
    for (int i = 0; i < 4; i++) {
      System.out.println(gen.nextString());
    }
  }
}
