package com.yahoo.ycsb.workloads;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;

public class TimeseriesWorkload extends Workload {  
  
  public static final String TIMESTAMP_KEY = "YCSBTS";
  public static final String VALUE_KEY = "YCSBV";
  
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
  
  public static final String KEY_LENGTH_PROPERTY = "key_length";
  public static final String KEY_LENGTH_PROPERTY_DEFAULT = "8";
  
  public static final String TAG_KEY_LENGTH_PROPERTY = "tag_key_length";
  public static final String TAG_KEY_LENGTH_PROPERTY_DEFAULT = "8";
  
  public static final String TAG_VALUE_LENGTH_PROPERTY = "tag_value_length";
  public static final String TAG_VALUE_LENGTH_PROPERTY_DEFAULT = "8";
  
  public static final String PAIR_DELIMITER_PROPERTY = "tag_pair_delimiter";
  public static final String PAIR_DELIMITER_PROPERTY_DEFAULT = "=";
  
  private Properties properties;
  
  private Generator<String> keyGenerator;
  private Generator<String> tagKeyGenerator;
  private Generator<String> tagValueGenerator;
  
  private int timestampInterval;
  private TimeUnit timeUnits;
  
  private int recordcount;
  private int tagPairs;
  private String table;
  
  private String[] keys;

  private int numKeys;
  private String[] tagKeys;
  private int[] tagCardinality;
  private String[][] tagValues;
  private int firstIncrementableCardinality;
  
  private String tagPairDelimiter;

  @Override
  public void init(final Properties p) throws WorkloadException {
    properties = p;
    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, 
            Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    // setup the key, tag key and tag value generators
    final int keyLength = Integer.parseInt(p.getProperty(KEY_LENGTH_PROPERTY, 
        KEY_LENGTH_PROPERTY_DEFAULT));
    final int tagKeyLength = Integer.parseInt(p.getProperty(
        TAG_KEY_LENGTH_PROPERTY, TAG_KEY_LENGTH_PROPERTY_DEFAULT));
    final int tagValueLength = Integer.parseInt(p.getProperty(
        TAG_VALUE_LENGTH_PROPERTY, TAG_VALUE_LENGTH_PROPERTY_DEFAULT));
    
    keyGenerator = new IncrementingPrintableStringGenerator(keyLength);
    tagKeyGenerator = new IncrementingPrintableStringGenerator(tagKeyLength);
    tagValueGenerator = new IncrementingPrintableStringGenerator(tagValueLength);
    
    // setup the cardinality
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

    for (int i = 0; i < tagPairs; ++i) {
      tagKeys[i] = tagKeyGenerator.nextString();
      
      int cardinality = tagCardinality[i];
      tagValues[i] = new String[cardinality];
      for (int x = 0; x < cardinality; ++x) {
        tagValues[i][x] = tagValueGenerator.nextString();
      }
    }
    
    // figure out the start timestamp based on the units, cardinality and interval
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
    
    tagPairDelimiter = p.getProperty(PAIR_DELIMITER_PROPERTY, PAIR_DELIMITER_PROPERTY_DEFAULT);
  }
  
  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    if (properties == null) {
      throw new WorkloadException("Workload has not been initialized.");
    }
    return new ThreadState(mythreadid, threadcount);
  }
  
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    if (threadstate == null) {
      //throw new WorkloadException("Missing thread state");
      return false;
    }
    final HashMap<String, ByteIterator> tags = new HashMap<String, ByteIterator>(tagPairs);
    final String key = ((ThreadState)threadstate).nextDataPoint(tags);
    if (db.insert(table, key, tags) == Status.OK) {
      return true;
    }
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    if (threadstate == null) {
      //throw new WorkloadException("Missing thread state");
      return false;
    }
    switch (((ThreadState)threadstate).operationchooser.nextString()) {
    case "READ":
      return doTransactionRead(db, threadstate);
    case "UPDATE":
      return doTransactionUpdate(db, threadstate);
    case "INSERT": 
      return doTransactionInsert(db, threadstate);
    case "SCAN":
      return doTransactionScan(db, threadstate);
    default:
      return doTransactionReadModifyWrite(db, threadstate);
    } 
  }

  protected boolean doTransactionRead(final DB db, Object threadstate) {
    final String keyname = keys[Utils.random().nextInt(keys.length)];
    final ThreadState state = (ThreadState)threadstate;
    
    final long timestamp = state.startTimestamp + 
        state.timestampGenerator.getOffset(Utils.random().nextInt(state.maxOffsets));
    
    // rando tags
    HashSet<String> fields = new HashSet<String>();
    for (int i = 0; i < tagPairs; ++i) {
      fields.add(tagKeys[i] + tagPairDelimiter + tagValues[i][Utils.random().nextInt(tagValues[i].length)]);
    }
    fields.add(TIMESTAMP_KEY + tagPairDelimiter + timestamp);
    
    final HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);
    
    return true;
  }
  
  protected boolean doTransactionUpdate(final DB db, Object threadstate) {
    if (threadstate == null) {
      //throw new WorkloadException("Missing thread state");
      return false;
    }
    final HashMap<String, ByteIterator> tags = new HashMap<String, ByteIterator>(tagPairs);
    final String key = ((ThreadState)threadstate).nextDataPoint(tags);
    if (db.update(table, key, tags) == Status.OK) {
      return true;
    }
    return false;
  }
  
  protected boolean doTransactionInsert(final DB db, Object threadstate) {
    return doInsert(db, threadstate);
  }
  
  protected boolean doTransactionScan(final DB db, Object threadstate) {
    return false;
  }
  
  protected boolean doTransactionReadModifyWrite(final DB db, Object threadstate) {
    return false;
  }
  
  protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
    
  }
  
  /**
   * Thread state class holding thread local generators and indices
   */
  protected class ThreadState {
    protected final UnixEpochTimestampGenerator timestampGenerator;
    protected final DiscreteGenerator operationchooser;
    
    private int keyIdx;
    private int keyIdxStart;
    private int keyIdxEnd;
    private int[] tagValueIdxs;

    private boolean rollover;
    long startTimestamp;
    final long interval;
    final long threadOps;
    int maxOffsets;
    
    ThreadState(final int threadID, final int threadCount) throws WorkloadException {
      if (threadID >= threadCount) {
        throw new IllegalStateException("Thread ID " + threadID + " cannot be greater "
            + "than or equal than the thread count " + threadCount);
      }
      if (keys.length < threadCount) {
        throw new WorkloadException("Thread count " + threadCount + " must be greater "
            + "than or equal to key count " + keys.length);
      }
      
      int keysPerThread = keys.length / threadCount;
      keyIdx = keyIdxStart = keysPerThread * threadID;
      if (threadCount - 1 == threadID) {
        keyIdxEnd = keys.length;
      } else {
        keyIdxEnd = keyIdxStart + keysPerThread;
      }
      
      tagValueIdxs = new int[tagPairs]; // all zeros
      
      final String startingTimestamp = 
          properties.getProperty(TIMESTAMP_START_PROPERTY);
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
      // Set the last value properly for the timestamp, otherwise it may start 
      // one interval ago.
      timestampGenerator.nextValue();
      startTimestamp = timestampGenerator.lastValue();
      interval = timestampGenerator.getOffset(1);
      threadOps = recordcount / threadCount;
      
      long recordsPerTimestamp = (keyIdxEnd - keyIdxStart);
      for (final int cardinality : tagCardinality) {
        recordsPerTimestamp *= cardinality;
      }
      maxOffsets = (int)((threadOps / recordsPerTimestamp) * interval);
      
      operationchooser = CoreWorkload.createOperationGenerator(properties);
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
      
      map.put(TIMESTAMP_KEY, new ByteArrayByteIterator(
          Utils.longToBytes(timestampGenerator.currentValue())));
      map.put(VALUE_KEY, new ByteArrayByteIterator(Utils.doubleToBytes(
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
        if (keyIdx + 1 >= keyIdxEnd) {
          keyIdx = keyIdxStart;
          rollover = true;
        } else {
          ++keyIdx;
        }
      }
      
      return key;
    }
  }
}
