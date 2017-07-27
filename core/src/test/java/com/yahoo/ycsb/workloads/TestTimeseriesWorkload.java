package com.yahoo.ycsb.workloads;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.TimeseriesWorkload.ThreadState;

import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class TestTimeseriesWorkload {
  
  @Test
  public void twoThreads() throws Exception {
    final Properties p = getUTProperties();
    Measurements.setProperties(p);
    
    final TimeseriesWorkload wl = new TimeseriesWorkload();
    wl.init(p);
    Object threadState = wl.initThread(p, 0, 2);
    
    MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeseriesWorkload.VALUE_KEY));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
    
    threadState = wl.initThread(p, 1, 2);
    db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAB");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeseriesWorkload.VALUE_KEY));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
  }
  
  @Test (expectedExceptions = WorkloadException.class)
  public void badTimeUnit() throws Exception {
    final Properties p = new Properties();
    p.put(TimeseriesWorkload.TIMESTAMP_UNITS_PROPERTY, "foobar");
    getWorkload(p, true);
  }
  
  @Test (expectedExceptions = WorkloadException.class)
  public void failedToInitWorkloadBeforeThreadInit() throws Exception {
    final Properties p = getUTProperties();
    final TimeseriesWorkload wl = getWorkload(p, false);
    //wl.init(p); // <-- we NEED this :(
    final Object threadState = wl.initThread(p, 0, 2);
    
    final MockDB db = new MockDB();
    wl.doInsert(db, threadState);
  }
  
  @Test
  public void failedToInitThread() throws Exception {
    final Properties p = getUTProperties();
    final TimeseriesWorkload wl = getWorkload(p, true);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertFalse(wl.doInsert(db, null));
    }
    
    assertEquals(db.keys.size(), 0);
    assertEquals(db.values.size(), 0);
  }
  
  @Test
  public void insertOneKeyTwoTagsLowCardinality() throws Exception {
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "1");
    final TimeseriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeseriesWorkload.VALUE_KEY)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
  }
  
  @Test
  public void insertTwoKeysTwoTagsLowCardinality() throws Exception {
    final Properties p = getUTProperties();
    
    final TimeseriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    int metricCtr = 0;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeseriesWorkload.VALUE_KEY)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
      }
      if (metricCtr++ > 1) {
        assertEquals(db.keys.get(i), "AAAB");
        if (metricCtr >= 4) {
          metricCtr = 0;
          timestamp += 60;
        }
      } else {
        assertEquals(db.keys.get(i), "AAAA");
      }
    }
  }
  
  @Test
  public void insertTwoKeysTwoThreads() throws Exception {
    final Properties p = getUTProperties();
    
    final TimeseriesWorkload wl = getWorkload(p, true);
    Object threadState = wl.initThread(p, 0, 2);
    
    MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA"); // <-- key 1
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeseriesWorkload.VALUE_KEY)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
    
    threadState = wl.initThread(p, 1, 2);
    db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAB"); // <-- key 2
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeseriesWorkload.VALUE_KEY));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
  }
  
  @Test
  public void insertThreeKeysTwoThreads() throws Exception {
    // To make sure the distribution doesn't miss any metrics
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "3");
    
    final TimeseriesWorkload wl = getWorkload(p, true);
    Object threadState = wl.initThread(p, 0, 2);
    
    MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertTrue(((NumericByteIterator) db.values.get(i)
          .get(TimeseriesWorkload.VALUE_KEY)).isFloatingPoint());
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
    
    threadState = wl.initThread(p, 1, 2);
    db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    timestamp = 1451606400;
    int metricCtr = 0;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertNotNull(db.values.get(i).get(TimeseriesWorkload.VALUE_KEY));
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
      }
      if (metricCtr++ > 1) {
        assertEquals(db.keys.get(i), "AAAC");
        if (metricCtr >= 4) {
          metricCtr = 0;
          timestamp += 60;
        }
      } else {
        assertEquals(db.keys.get(i), "AAAB");
      }
    }
  }
  
  @Test
  public void insertWithValidation() throws Exception {
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "1");
    p.put(CoreWorkload.DATA_INTEGRITY_PROPERTY, "true");
    final TimeseriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 74; i++) {
      assertTrue(wl.doInsert(db, threadState));
    }
    
    assertEquals(db.keys.size(), 74);
    assertEquals(db.values.size(), 74);
    long timestamp = 1451606400;
    for (int i = 0; i < db.keys.size(); i++) {
      assertEquals(db.keys.get(i), "AAAA");
      assertEquals(db.values.get(i).get("AA").toString(), "AAAA");
      assertEquals(Utils.bytesToLong(db.values.get(i).get(
          TimeseriesWorkload.TIMESTAMP_KEY).toArray()), timestamp);
      assertFalse(((NumericByteIterator) db.values.get(i)
          .get(TimeseriesWorkload.VALUE_KEY)).isFloatingPoint());
      
      // validation check
      final TreeMap<String, String> validationTags = new TreeMap<String, String>();
      for (final Entry<String, ByteIterator> entry : db.values.get(i).entrySet()) {
        if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY) || 
            entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
          continue;
        }
        validationTags.put(entry.getKey(), entry.getValue().toString());
      }
      assertEquals(wl.validationFunction(db.keys.get(i), timestamp, validationTags), 
          ((NumericByteIterator) db.values.get(i).get(TimeseriesWorkload.VALUE_KEY)).getLong());
      
      if (i % 2 == 0) {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAB");
      } else {
        assertEquals(db.values.get(i).get("AB").toString(), "AAAC");
        timestamp += 60;
      }
    }
  }
  
  @Test
  public void read() throws Exception {
    final Properties p = getUTProperties();
    final TimeseriesWorkload wl = getWorkload(p, true);
    final Object threadState = wl.initThread(p, 0, 1);
    
    final MockDB db = new MockDB();
    for (int i = 0; i < 20; i++) {
      wl.doTransactionRead(db, threadState);
    }
  }
  
  @Test
  public void verifyRow() throws Exception {
    final Properties p = getUTProperties();
    final TimeseriesWorkload wl = getWorkload(p, true);
    
    final TreeMap<String, String> validationTags = new TreeMap<String, String>();
    final HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    
    validationTags.put("AA", "AAAA");
    cells.put("AA", new StringByteIterator("AAAA"));
    validationTags.put("AB", "AAAB");
    cells.put("AB", new StringByteIterator("AAAB"));
    long hash = wl.validationFunction("AAAA", 1451606400L, validationTags);
        
    cells.put(TimeseriesWorkload.TIMESTAMP_KEY, new NumericByteIterator(1451606400L));
    cells.put(TimeseriesWorkload.VALUE_KEY, new NumericByteIterator(hash));
    
    assertEquals(wl.verifyRow("AAAA", cells), Status.OK);
    
    // tweak the last value a bit
    for (final ByteIterator it : cells.values()) {
      it.reset();
    }
    cells.put(TimeseriesWorkload.VALUE_KEY, new NumericByteIterator(hash + 1));
    assertEquals(wl.verifyRow("AAAA", cells), Status.UNEXPECTED_STATE);
    
    // no value cell, returns an unexpected state
    for (final ByteIterator it : cells.values()) {
      it.reset();
    }
    cells.remove(TimeseriesWorkload.VALUE_KEY);
    assertEquals(wl.verifyRow("AAAA", cells), Status.UNEXPECTED_STATE);
  }
  
  @Test
  public void threadState() throws Exception {
    final Properties p = getUTProperties();
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "4");
    final TimeseriesWorkload wl = getWorkload(p, true);
    final ThreadState threadState = (ThreadState) wl.initThread(p, 0, 1);
    
    System.out.println(threadState.maxOffsets);
  }
  
  /** Helper method that generates unit testing defaults for the properties map */
  private Properties getUTProperties() {
    final Properties p = new Properties();
    p.put(Client.RECORD_COUNT_PROPERTY, "10");
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "2");
    p.put(TimeseriesWorkload.KEY_LENGTH_PROPERTY, "4");
    p.put(TimeseriesWorkload.TAG_KEY_LENGTH_PROPERTY, "2");
    p.put(TimeseriesWorkload.TAG_VALUE_LENGTH_PROPERTY, "4");
    p.put(TimeseriesWorkload.TAG_COUNT_PROPERTY, "2");
    p.put(TimeseriesWorkload.TAG_CARDINALITY_PROPERTY, "1,2");
    p.put(CoreWorkload.INSERT_START_PROPERTY, "1451606400");
    return p;
  }
  
  /** Helper to setup the workload for testing. */
  private TimeseriesWorkload getWorkload(final Properties p, final boolean init) 
      throws WorkloadException {
    Measurements.setProperties(p);
    if (!init) {
      return new TimeseriesWorkload();
    } else {
      final TimeseriesWorkload workload = new TimeseriesWorkload();
      workload.init(p);
      return workload;
    }
  }
  
  static class MockDB extends DB {
    final List<String> keys = new ArrayList<String>();
    final List<HashMap<String, ByteIterator>> values = 
        new ArrayList<HashMap<String, ByteIterator>>();
    
    @Override
    public Status read(String table, String key, Set<String> fields,
        HashMap<String, ByteIterator> result) {
      System.out.println("Table; " + table + "  Key: " + key + "  Fields: " + fields);
      return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status update(String table, String key,
        HashMap<String, ByteIterator> values) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status insert(String table, String key,
        HashMap<String, ByteIterator> values) {
      keys.add(key);
      this.values.add(values);
      return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
      // TODO Auto-generated method stub
      return Status.OK;
    }
    
    public void dumpStdout() {
      for (int i = 0; i < keys.size(); i++) {
        System.out.print("[" + i + "] Key: " + keys.get(i) + " Values: {");
        int x = 0;
        for (final Entry<String, ByteIterator> entry : values.get(i).entrySet()) {
          if (x++ > 0) {
            System.out.print(", ");
          }
          System.out.print("{" + entry.getKey() + " => ");
          if (entry.getKey().equals("YCSBV")) {
            System.out.print(new String(Utils.bytesToDouble(entry.getValue().toArray()) + "}"));  
          } else if (entry.getKey().equals("YCSBTS")) {
            System.out.print(new String(Utils.bytesToLong(entry.getValue().toArray()) + "}"));
          } else {
            System.out.print(new String(entry.getValue().toArray()) + "}");
          }
        }
        System.out.println("}");
      }
    }
  }
}
