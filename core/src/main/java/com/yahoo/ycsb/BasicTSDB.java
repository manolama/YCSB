package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.HashSet;
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
  private long lastTimestamp;
  
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
      Set<String> filtered = null;
      if (fields != null) {
        filtered = new HashSet<String>();
        for (final String field : fields) {
          if (field.startsWith(TimeseriesWorkload.TIMESTAMP_KEY)) {
            String[] parts = field.split(tagPairDelimiter);
            if (parts[1].contains(queryTimeSpanDelimiter)) {
              parts = parts[1].split(queryTimeSpanDelimiter);
              lastTimestamp = Long.parseLong(parts[0]);
            } else {
              lastTimestamp = Long.parseLong(parts[1]);
            }
            synchronized(TIMESTAMPS) {
              Integer ctr = TIMESTAMPS.get(lastTimestamp);
              if (ctr == null) {
                TIMESTAMPS.put(lastTimestamp, 1);
              } else {
                TIMESTAMPS.put(lastTimestamp, ctr + 1);
              }
            }
          } else {
            filtered.add(field);
          }
        }
      }
      incCounter(READS, hash(table, key, filtered));
    }
    return Status.OK;
  }
  
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    delay();

    boolean isFloat = false;
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        final TreeMap<String, ByteIterator> tree = new TreeMap<String, ByteIterator>(values);
        for (Map.Entry<String, ByteIterator> entry : tree.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
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
        Integer ctr = TIMESTAMPS.get(lastTimestamp);
        if (ctr == null) {
          TIMESTAMPS.put(lastTimestamp, 1);
        } else {
          TIMESTAMPS.put(lastTimestamp, ctr + 1);
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
    
    boolean isFloat = false;
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        final TreeMap<String, ByteIterator> tree = new TreeMap<String, ByteIterator>(values);
        for (Map.Entry<String, ByteIterator> entry : tree.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
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
        Integer ctr = TIMESTAMPS.get(lastTimestamp);
        if (ctr == null) {
          TIMESTAMPS.put(lastTimestamp, 1);
        } else {
          TIMESTAMPS.put(lastTimestamp, ctr + 1);
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
      
      long minTs = Long.MAX_VALUE;
      long maxTs = Long.MIN_VALUE;
      for (final long ts : TIMESTAMPS.keySet()) {
        if (ts > maxTs) {
          maxTs = ts;
        }
        if (ts < minTs) {
          minTs = ts;
        }
      }
      System.out.println("[TIMESTAMPS], Min, " + minTs);
      System.out.println("[TIMESTAMPS], Max, " + maxTs);
    }
  }
  
  @Override
  protected int hash(final String table, final String key, final HashMap<String, ByteIterator> values) {
    final TreeMap<String, ByteIterator> sorted = new TreeMap<String, ByteIterator>();
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
        continue;
      } else if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
        lastTimestamp = ((NumericByteIterator) entry.getValue()).getLong();
        entry.getValue().reset();
        continue;
      }
      sorted.put(entry.getKey(), entry.getValue());
    }
//    return Objects.hash(table, key, sorted);
    StringBuilder buf = new StringBuilder().append(table).append(key);
    for (final Entry<String, ByteIterator> entry : sorted.entrySet()) {
      entry.getValue().reset();
      buf.append(entry.getKey())
         .append(entry.getValue().toString());
    }
    return buf.toString().hashCode();
  }
  
}
