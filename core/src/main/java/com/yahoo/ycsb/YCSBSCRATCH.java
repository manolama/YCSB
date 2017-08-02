package com.yahoo.ycsb;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import com.yahoo.ycsb.generator.RandomDiscreteTimestampGenerator;
import com.yahoo.ycsb.generator.RandomPrintableStringGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.UniformGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

public class YCSBSCRATCH {

  public static void main(String[] args) {
    sparsity();
    //randoTimestamps();
    //beano();
    //randomPrintableStringGenerator();
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
  
  static void beano() {
    NotificationListener printListener = new NotificationListener() {
      public void handleNotification(Notification n, Object handback) {
          if (!(n instanceof MBeanServerNotification)) {
              System.out.println("Ignored notification of class " + n.getClass().getName());
              return;
          }
          MBeanServerNotification mbsn = (MBeanServerNotification) n;
          String what;
          if (n.getType().equals(MBeanServerNotification.REGISTRATION_NOTIFICATION))
              what = "MBean registered";
          else if (n.getType().equals(MBeanServerNotification.UNREGISTRATION_NOTIFICATION))
              what = "MBean unregistered";
          else
              what = "Unknown type " + n.getType();
          System.out.println("Received MBean Server notification: " + what + ": " +
                  mbsn.getMBeanName());
      }
  };
  
    try {
      ObjectName on = ManagementFactory.getGarbageCollectorMXBeans().get(0).getObjectName();
      System.out.println("Domain: " + on.toString());
      Hashtable<String,String> table = new Hashtable<String, String>();
      table.put("type", "GarbageCollector");
      table.put("name", "*");
      ObjectName on2 = new ObjectName("java.lang", table);
    ManagementFactory.getPlatformMBeanServer().addNotificationListener(
        on2, printListener, null, null);
    } catch (MalformedObjectNameException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

 catch (InstanceNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  static void testCharSets() {
    //  final int[] characterSet = RandomPrintableStringGenerator.generateCharacterSet(
    //  127, null, false, RandomPrintableStringGenerator.ALLBUTCONTROL);
    final int[] characterSet = IncrementingPrintableStringGenerator.fullPrintableBasicASCIISet();
    for (int c : characterSet) {
    System.out.println("Char: [" + new String(Character.toChars(c)) + "] "
        + "cp [" + c + "]  type [" + Character.getType(c) + "] isAlpha [" + 
        Character.isAlphabetic(c) + "]");
    }
    System.out.println("Total set size: " + characterSet.length);
  }
  
  static void randomPrintableStringGenerator() {
    RandomPrintableStringGenerator gen = //new RandomPrintableStringGenerator();
        new RandomPrintableStringGenerator(256, 
            IncrementingPrintableStringGenerator.fullPrintablePlaneZeroSet());
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

  static void randoTimestamps() {
    RandomDiscreteTimestampGenerator gen = 
        new RandomDiscreteTimestampGenerator(60, TimeUnit.SECONDS, 1483272000, 60);
    
    for (int i = 0; i < 60; i++) {
      System.out.println(gen.nextValue());
    }
    
  }
  
  static void sparsity() {
    List<Integer> set = new ArrayList<Integer>();
    set.add(0);
    set.add(1);
    set.add(2);
    set.add(3);
    set.add(4);
    set.add(5);
    set.add(6);
    set.add(7);
    set.add(8);
    set.add(9);
    
    double sparsity = 0.15; // % we want to skip
    double idx = 0;
    double inc = ((double) set.size() * sparsity);
    idx = inc;
    System.out.println("INC: " + inc);
    for (int i = 0; i < 20; i++) {
      if (Math.round(idx) >= set.size()) {
        idx = (idx - (double) set.size());
        System.out.println("--------- rolled: " + idx);
      }
      System.out.println("PICK: " + set.get((int) Math.round(idx)));
      idx += inc;
    }
  }
}
