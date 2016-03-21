package com.yahoo.ycsb.workloads;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.OrderedPrintableStringGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;

public class TimeseriesWorkload extends Workload {  
  /** Name and default value for the timestamp interval property. */    
  public static final String TIMESTAMP_INTERVAL_PROPERTY = "timestamp_interval";    
  public static final String TIMESTAMP_INTERVAL_PROPERTY_DEFAULT = "60";    
      
  /** Name and default value for the timestamp units property. */   
  public static final String TIMESTAMP_UNITS_PROPERTY = "timestamp_units";    
  public static final String TIMESTAMP_UNITS_PROPERTY_DEFAULT = "SECONDS";    
      
  /** Name for the optional starting timestamp property. */   
  public static final String TIMESTAMP_START_PROPERTY = "start_timestamp";    
     
  
  public static final String TAG_COUNT_PROPERTY = "tag_count";
  public static final String TAG_COUNT_PROPERTY_DEFAULT = "4";
  
  public static final String TAG_CARDINALITY_PROPERTY = "tag_cardinality";
  public static final String TAG_CARDINALITY_PROPERTY_DEFAULT = "1, 2, 4, 8";
  
  private Generator<String> keyGenerator;
  private Generator<String> tagKeyGenerator;
  private Generator<String> tagValueGenerator;
  private int recordcount;
  private int tagPairs;
  private String table;
  private UnixEpochTimestampGenerator timestampGenerator;
  
  private String[] keys;
  private int keyIdx;
  private int numKeys;
  private String[] tagKeys;
  private int[] tagCardinality;
  private String[][] tagValues;
  private int[] tagValueIdxs;
  private int firstIncrementableCardinality;
  private boolean rollover;
  
  @Override
  public void init(Properties p) throws WorkloadException {
    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, 
            Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    
    keyGenerator = new OrderedPrintableStringGenerator(4);
    tagKeyGenerator = new OrderedPrintableStringGenerator(2);
    tagValueGenerator = new OrderedPrintableStringGenerator(4);
    tagPairs = Integer.parseInt(p.getProperty(TAG_COUNT_PROPERTY, 
        TAG_COUNT_PROPERTY_DEFAULT));
    tagCardinality = new int[tagPairs];
    
    final String tagCardinalityString = p.getProperty(TAG_CARDINALITY_PROPERTY, 
        TAG_CARDINALITY_PROPERTY_DEFAULT);
    final String[] tagCardinalityParts = tagCardinalityString.split(",");
    int idx = 0;
    for (final String cardinality : tagCardinalityParts) {
      try {
        tagCardinality[idx] = Integer.parseInt(cardinality.trim());
      } catch (NumberFormatException nfe) {
        throw new WorkloadException("Unable to parse cardinality: " + 
            cardinality, nfe);
      }
      if (tagCardinality[idx] < 1) {
        throw new WorkloadException("Cardinality must be greater than zero: " + 
            tagCardinality[idx]);
      }
      ++idx;
      if (idx >= tagPairs) {
        // we have more cardinalities than tag keys so bail at this point.
        break;
      }
    }
    // fill tags without explicit cardinality with 1
    if (idx < tagPairs) {
      tagCardinality[idx++] = 1;
    }
    
    for (int i = 0; i < tagCardinality.length; ++i) {
      if (tagCardinality[i] > 1) {
        firstIncrementableCardinality = i;
        break;
      }
    }
    numKeys = Integer.parseInt(p.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, 
        CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    
    keys = new String[numKeys];
    for (int i = 0; i < numKeys; ++i) {
      keys[i] = keyGenerator.nextString();
    }
    
    tagKeys = new String[tagPairs];
    tagValues = new String[tagPairs][];
    tagValueIdxs = new int[tagPairs]; // all zeros
    for (int i = 0; i < tagPairs; ++i) {
      tagKeys[i] = tagKeyGenerator.nextString();
      
      int cardinality = tagCardinality[i];
      tagValues[i] = new String[cardinality];
      for (int x = 0; x < cardinality; ++x) {
        tagValues[i][x] = tagValueGenerator.nextString();
      }
    }
    
    // figure out the start timestamp based on the units, cardinality and interval
    TimeUnit timeUnits;
    int timestampInterval = 0;
    
    try {
      timestampInterval = Integer.parseInt(p.getProperty(
          TIMESTAMP_INTERVAL_PROPERTY, TIMESTAMP_INTERVAL_PROPERTY_DEFAULT));
    } catch (NumberFormatException nfe) {
      throw new WorkloadException("Unable to parse the " + 
          TIMESTAMP_INTERVAL_PROPERTY, nfe);
    }
    
    try {
      
      timeUnits = TimeUnit.valueOf(p.getProperty(TIMESTAMP_UNITS_PROPERTY, 
          TIMESTAMP_UNITS_PROPERTY_DEFAULT).toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new WorkloadException("Unknown time unit type", e);
    }
    if (timeUnits == TimeUnit.NANOSECONDS || timeUnits == TimeUnit.MICROSECONDS) {
      throw new WorkloadException("YCSB doesn't support " + timeUnits + 
          " at this time.");
    }
    final String startingTimestamp = 
        p.getProperty(TIMESTAMP_START_PROPERTY);
    if (startingTimestamp == null || startingTimestamp.isEmpty()) {
      timestampGenerator = new UnixEpochTimestampGenerator(timestampInterval, timeUnits);
    } else {
      try {
        timestampGenerator = new UnixEpochTimestampGenerator(timestampInterval, timeUnits, 
            Long.parseLong(startingTimestamp));
      } catch (NumberFormatException nfe) {
        throw new WorkloadException("Unable to parse the " + 
            TIMESTAMP_START_PROPERTY, nfe);
      }
    }
  }
  
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    final HashMap<String, ByteIterator> tags = new HashMap<String, ByteIterator>(tagPairs);
    final String key = nextDataPoint(tags);
    if (db.insert(table, key, tags) == Status.OK) {
      return true;
    }
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    // TODO Auto-generated method stub
    return false;
  }

  private String nextDataPoint(HashMap<String, ByteIterator> map) {
    if (rollover) {
      timestampGenerator.nextValue();
      rollover = false;
    }
    
    final String key = keys[keyIdx];
    for (int i = 0; i < tagPairs; ++i) {
      int tvidx = tagValueIdxs[i];
      map.put(tagKeys[i], new StringByteIterator(tagValues[i][tvidx]));
    }
    // TODO - byte array
    map.put("YCSBTS", new ByteArrayByteIterator(Utils.longToBytes(timestampGenerator.currentValue())));
    map.put("YCSBV", new ByteArrayByteIterator(Utils.doubleToBytes(
        Utils.random().nextDouble() * 100000)));
    
    boolean tagRollover = false;
    for (int i = tagCardinality.length - 1; i >= 0; --i) {
      if (tagCardinality[i] <= 1) {
        // nothing to increment here
        continue;
      }
      
      if (tagValueIdxs[i] + 1 >= tagCardinality[i]) {
        tagValueIdxs[i] = 0;
        if (i == firstIncrementableCardinality) {
          tagRollover = true;
        }
      } else {
         ++tagValueIdxs[i];
         break;
      }
    }
    
    if (tagRollover) {
      if (keyIdx + 1 >= keys.length) {
        keyIdx = 0;
        rollover = true;
      } else {
        ++keyIdx;
      }
    }
    
    return key;
  }
  
}
