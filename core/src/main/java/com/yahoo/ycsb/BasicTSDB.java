package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.workloads.TimeseriesWorkload;

public class BasicTSDB extends BasicDB {

  protected static Map<Long, Integer> TIMESTAMPS;
  protected static Map<Integer, Integer> FLOATS;
  protected static Map<Integer, Integer> INTEGERS;
  
  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  
  @Override
  public void init() {
    super.init();
    
    synchronized (MUTEX) {
      if (TIMESTAMPS == null) {
        TIMESTAMPS = new HashMap<Long, Integer>();
        FLOATS = new HashMap<Integer, Integer>();
        INTEGERS = new HashMap<Integer, Integer>();
      }
    }
    
    tagPairDelimiter = getProperties().getProperty(
        TimeseriesWorkload.PAIR_DELIMITER_PROPERTY, 
        TimeseriesWorkload.PAIR_DELIMITER_PROPERTY_DEFAULT);
    queryTimeSpanDelimiter = getProperties().getProperty(
        TimeseriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY,
        TimeseriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
  }
  
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    delay();

    long timestamp = 0;
    boolean isFloat = false;
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
            if (count) {
              timestamp = Utils.bytesToLong(entry.getValue().toArray());
              entry.getValue().reset();
            }
            sb.append(entry.getKey()).append("=").append(Utils.bytesToLong(entry.getValue().toArray())).append(" ");
          } else if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            isFloat = it.isFloatingPoint();
            sb.append(entry.getKey()).append("=").append(it.isFloatingPoint() ? it.getDouble() : it.getLong()).append(" ");
          } else {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
          }
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      int hash = hash(table, key, values);
      incCounter(UPDATES, hash);
      synchronized(TIMESTAMPS) {
        Integer ctr = TIMESTAMPS.get(timestamp);
        if (ctr == null) {
          TIMESTAMPS.put(timestamp, 1);
        } else {
          TIMESTAMPS.put(timestamp, ctr + 1);
        }
      }
      if (isFloat) {
        incCounter(FLOATS, hash);
      } else {
        incCounter(INTEGERS, hash);
      }
    }
    
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    delay();

    long timestamp = 0;
    boolean isFloat = false;
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
            if (count) {
              timestamp = Utils.bytesToLong(entry.getValue().toArray());
              entry.getValue().reset();
            }
            sb.append(entry.getKey()).append("=").append(Utils.bytesToLong(entry.getValue().toArray())).append(" ");
          } else if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            isFloat = it.isFloatingPoint();
            sb.append(entry.getKey()).append("=").append(it.isFloatingPoint() ? it.getDouble() : it.getLong()).append(" ");
          } else {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
          }
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      int hash = hash(table, key, values);
      incCounter(INSERTS, hash);
      synchronized(TIMESTAMPS) {
        Integer ctr = TIMESTAMPS.get(timestamp);
        if (ctr == null) {
          TIMESTAMPS.put(timestamp, 1);
        } else {
          TIMESTAMPS.put(timestamp, ctr + 1);
        }
      }
      if (isFloat) {
        incCounter(FLOATS, hash);
      } else {
        incCounter(INTEGERS, hash);
      }
    }

    return Status.OK;
  }

  @Override
  public void cleanup() {
    super.cleanup();
    if (count && COUNTER < 1) {
      System.out.println("[TIMESTAMPS], Unique, " + TIMESTAMPS.size());
      System.out.println("[FLOATS], Unique, " + FLOATS.size());
      System.out.println("[INTEGERS], Unique, " + INTEGERS.size());
    }
  }
  
  @Override
  protected int hash(final String table, final String key, final HashMap<String, ByteIterator> values) {
    final TreeMap<String, ByteIterator> sorted = new TreeMap<String, ByteIterator>();
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY) || 
          entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
        continue;
      }
      sorted.put(entry.getKey(), entry.getValue());
    }
    return Objects.hash(table, key, sorted);
  }
  
}
