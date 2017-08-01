/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.HdrHistogram.AbstractHistogram.LinearBucketValues;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.Recorder;

import com.yahoo.ycsb.Workload.Operation;

/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class BasicDB extends DB {
  public static final String COUNT = "basicdb.counts";
  public static final String COUNT_DEFAULT = "false";
  
  public static final String VERBOSE = "basicdb.verbose";
  public static final String VERBOSE_DEFAULT = "true";

  public static final String SIMULATE_DELAY = "basicdb.simulatedelay";
  public static final String SIMULATE_DELAY_DEFAULT = "0";

  public static final String RANDOMIZE_DELAY = "basicdb.randomizedelay";
  public static final String RANDOMIZE_DELAY_DEFAULT = "true";

  protected static final Object MUTEX = new Object();
  protected static int COUNTER = 0;
  
  protected static Map<Integer, Integer> READS;
  protected static Map<Integer, Integer> SCANS;
  protected static Map<Integer, Integer> UPDATES;
  protected static Map<Integer, Integer> INSERTS;
  protected static Map<Integer, Integer> DELETES;
  
  protected boolean verbose;
  protected boolean randomizedelay;
  protected int todelay;
  protected boolean count;

  public BasicDB() {
    todelay = 0;
  }

  protected void delay() {
    if (todelay > 0) {
      long delayNs;
      if (randomizedelay) {
        delayNs = TimeUnit.MILLISECONDS.toNanos(Utils.random().nextInt(todelay));
        if (delayNs == 0) {
          return;
        }
      } else {
        delayNs = TimeUnit.MILLISECONDS.toNanos(todelay);
      }

      final long deadline = System.nanoTime() + delayNs;
      do {
        LockSupport.parkNanos(deadline - System.nanoTime());
      } while (System.nanoTime() < deadline && !Thread.interrupted());
    }
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() {
    verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
    todelay = Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
    randomizedelay = Boolean.parseBoolean(getProperties().getProperty(RANDOMIZE_DELAY, RANDOMIZE_DELAY_DEFAULT));
    count = Boolean.parseBoolean(getProperties().getProperty(COUNT, COUNT_DEFAULT));
    
    synchronized (MUTEX) {
      if (COUNTER == 0 && count) {
        READS = new HashMap<Integer, Integer>();
        SCANS = new HashMap<Integer, Integer>();
        UPDATES = new HashMap<Integer, Integer>();
        INSERTS = new HashMap<Integer, Integer>();
        DELETES = new HashMap<Integer, Integer>();
      }
      COUNTER++;
    }
    
    if (verbose) {
      synchronized (System.out) {
        System.out.println("***************** properties *****************");
        Properties p = getProperties();
        if (p != null) {
          for (Enumeration e = p.propertyNames(); e.hasMoreElements();) {
            String k = (String) e.nextElement();
            System.out.println("\"" + k + "\"=\"" + p.getProperty(k) + "\"");
          }
        }
        System.out.println("**********************************************");
      }
    }
  }

  protected static final ThreadLocal<StringBuilder> TL_STRING_BUILDER = new ThreadLocal<StringBuilder>() {
    @Override
    protected StringBuilder initialValue() {
      return new StringBuilder();
    }
  };

  protected static StringBuilder getStringBuilder() {
    StringBuilder sb = TL_STRING_BUILDER.get();
    sb.setLength(0);
    return sb;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    delay();
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("READ ").append(table).append(" ").append(key).append(" [ ");
      if (fields != null) {
        for (String f : fields) {
          sb.append(f).append(" ");
        }
      } else {
        sb.append("<all fields>");
      }

      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(READS, hash(table, key, fields));
    }
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    delay();
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("SCAN ").append(table).append(" ").append(startkey).append(" ").append(recordcount).append(" [ ");
      if (fields != null) {
        for (String f : fields) {
          sb.append(f).append(" ");
        }
      } else {
        sb.append("<all fields>");
      }

      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(SCANS, hash(table, startkey, fields));
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    delay();
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(UPDATES, hash(table, key, values));
    }
    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    delay();
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
      }

      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      incCounter(INSERTS, hash(table, key, values));
    }

    return Status.OK;
  }


  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public Status delete(String table, String key) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("DELETE ").append(table).append(" ").append(key);
      System.out.println(sb);
    }

    if (count) {
      incCounter(DELETES, (table + key).hashCode());
    }
    
    return Status.OK;
  }
  
  @Override
  public void cleanup() {
    synchronized (MUTEX) {
      int countDown = --COUNTER;
      if (count && countDown < 1) {
//        Recorder recorder = new Recorder(3);
//        long max = 0;
//        for (final Entry<Integer, Integer> entry : INSERTS.entrySet()) {
////          recorder.recordValue(value);
////          if (value > max) {
////            max = value;
////          }
//          System.out.println("Hash: " + entry.getKey() + "  C: " + entry.getValue());
//        }
//        final Histogram h = recorder.getIntervalHistogram();
//        long sum = 0;
//        System.out.println(".......................");
//        System.out.flush();
//        //System.out.println("HTV: " + max);
//        long v = max / 50;
//        for (int i = 0; i < 50; i++) {
//          long c = h.getCountBetweenValues(v * i, (v * i + 1));
//          System.out.println("HMM: " + c);
//          sum += c;
//        }
////        ByteArrayOutputStream baos = new ByteArrayOutputStream();
////        PrintStream ps = new PrintStream(baos);
////        h.outputPercentileDistribution(ps, 1.0);
////        try {
////          System.out.println("DIST: " + baos.toString("UTF8"));
////        } catch (UnsupportedEncodingException e) {
////          // TODO Auto-generated catch block
////          e.printStackTrace();
////        }
////        for (int i = 0; i < 105; i++) {
////          //System.out.println("[H], v, " + i + "%  " + h.getValueAtPercentile((double) i));
////          long v = h.getCountAtValue(i);
////          sum += v;
////          System.out.println("[H], v, " + i + "%  " + v);
////        }
//        System.out.println("------------- " + sum);
        System.out.println("[READS], Uniques, " + READS.size());
        System.out.println("[SCANS], Uniques, " + SCANS.size());
        System.out.println("[UPDATES], Uniques, " + UPDATES.size());
        System.out.println("[INSERTS], Uniques, " + INSERTS.size());
        System.out.println("[DELETES], Uniques, " + DELETES.size());
      }
    }
  }
  
  protected void incCounter(final Map<Integer, Integer> map, final int hash) {
    synchronized (map) {
      Integer ctr = map.get(hash);
      if (ctr == null) {
        map.put(hash, 1);
      } else {
        map.put(hash, ctr + 1);
      }
    }
  }
  
  protected int hash(final String table, final String key, final Set<String> fields) {
    if (fields == null) {
      return Objects.hash(table, key);
    }
    List<String> sorted = new ArrayList<String>(fields);
    return Objects.hash(table, key, sorted);
  }
  
  protected int hash(final String table, final String key, final HashMap<String, ByteIterator> values) {
    if (values == null) {
      return Objects.hash(table, key);
    }
    final TreeMap<String, ByteIterator> sorted = new TreeMap<String, ByteIterator>(values);
    
    return Objects.hash(table, key, sorted);
  }
  /**
   * Short test of BasicDB
   */
  /*
  public static void main(String[] args) {
    BasicDB bdb = new BasicDB();

    Properties p = new Properties();
    p.setProperty("Sky", "Blue");
    p.setProperty("Ocean", "Wet");

    bdb.setProperties(p);

    bdb.init();

    HashMap<String, String> fields = new HashMap<String, String>();
    fields.put("A", "X");
    fields.put("B", "Y");

    bdb.read("table", "key", null, null);
    bdb.insert("table", "key", fields);

    fields = new HashMap<String, String>();
    fields.put("C", "Z");

    bdb.update("table", "key", fields);

    bdb.delete("table", "key");
  }
  */
}
