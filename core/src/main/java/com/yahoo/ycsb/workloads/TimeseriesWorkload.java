package com.yahoo.ycsb.workloads;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import com.yahoo.ycsb.generator.RandomDiscreteTimestampGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.measurements.Measurements;

public class TimeseriesWorkload extends Workload {  
  
  public enum ValueType {
    INTEGERS("integers"),
    FLOATS("floats"),
    MIXED("mixed");
    
    private final String name;
    
    ValueType(final String name) {
      this.name = name;
    }
    
    public static ValueType fromString(final String name) {
      for (final ValueType type : ValueType.values()) {
        if (type.name.equalsIgnoreCase(name)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unrecognized type: " + name);
    }
  }
  
  public static final String TIMESTAMP_KEY = "YCSBTS";
  public static final String VALUE_KEY = "YCSBV";
  
  /** Name and default value for the timestamp interval property. */    
  public static final String TIMESTAMP_INTERVAL_PROPERTY = "timestamp_interval";    
  public static final String TIMESTAMP_INTERVAL_PROPERTY_DEFAULT = "60";    
      
  /** Name and default value for the timestamp units property. */   
  public static final String TIMESTAMP_UNITS_PROPERTY = "timestamp_units";    
  public static final String TIMESTAMP_UNITS_PROPERTY_DEFAULT = "SECONDS"; 
  
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
  
  public static final String RANDOMIZE_TIMESTAMP_ORDER_PROPERTY = "randomize_timestamp_order";
  public static final String RANDOMIZE_TIMESTAMP_ORDER_PROPERTY_DEFAULT = "false";
  
  public static final String RANDOMIZE_TIMESERIES_ORDER_PROPERTY = "randomize_timeseries_order";
  public static final String RANDOMIZE_TIMESERIES_ORDER_PROPERTY_DEFAULT = "true";
  
  public static final String VALUE_TYPE_PROPERTY = "value_type";
  public static final String VALUE_TYPE_PROPERTY_DEFAULT = "floating";
  
  // Query params
  public static final String QUERY_TIMESPAN_PROPERTY = "query_timespan";
  public static final String QUERY_TIMESPAN_PROPERTY_DEFAULT = "3600";
  
  public static final String QUERY_TIMESPAN_DELIMITER_PROPERTY = "query_timespan_delimiter";
  public static final String QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT = ",";
  
  public static final String GROUPBY_KEY_PROPERTY = "group_by_key";
  public static final String GROUPBY_KEY_PROPERTY_DEFAULT = "YCSBGB";
  
  public static final String GROUPBY_PROPERTY = "group_by_function";
  
  public static final String GROUPBY_KEYS_PROPERTY = "group_by_keys";
  
  public static final String DOWNSAMPLING_KEY_PROPERTY = "downsampling_key";
  public static final String DOWNSAMPLING_KEY_PROPERTY_DEFAULT = "YCSBDS";
  
  public static final String DOWNSAMPLING_PROPERTY = "downsampling_function";
  
  private Properties properties;
  
  private Generator<String> keyGenerator;
  private Generator<String> tagKeyGenerator;
  private Generator<String> tagValueGenerator;
  
  private int timestampInterval;
  private TimeUnit timeUnits;
  private boolean randomizeTimestampOrder;
  private boolean randomizeTimeseriesOrder;
  private ValueType valueType;
  private int totalCardinality;
  
  private int recordcount;
  private int tagPairs;
  private String table;
  
  private String[] keys;

  private int numKeys;
  private String[] tagKeys;
  private int[] tagCardinality;
  private String[][] tagValues;
  private int firstIncrementableCardinality;
  
  // Query parameters
  private int queryTimeSpan;
  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  private boolean groupBy;
  private String groupByKey;
  private String groupByFunction;
  private boolean[] groupBys;
  private boolean downsample;
  private String downsampleKey;
  private String downsampleFunction;

  /**
   * Set to true if want to check correctness of reads. Must also
   * be set to true during loading phase to function.
   */
  private boolean dataintegrity;
  
  private Measurements _measurements = Measurements.getMeasurements();
  
  @Override
  public void init(final Properties p) throws WorkloadException {
    properties = p;
    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, 
            Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    
    randomizeTimestampOrder = Boolean.parseBoolean(p.getProperty(
        RANDOMIZE_TIMESTAMP_ORDER_PROPERTY, 
        RANDOMIZE_TIMESTAMP_ORDER_PROPERTY_DEFAULT));
    randomizeTimeseriesOrder = Boolean.parseBoolean(p.getProperty(
        RANDOMIZE_TIMESERIES_ORDER_PROPERTY, 
        RANDOMIZE_TIMESERIES_ORDER_PROPERTY_DEFAULT));
    
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
    int threads = Integer.parseInt(p.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    numKeys = Integer.parseInt(p.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, 
        CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    tagPairs = Integer.parseInt(p.getProperty(TAG_COUNT_PROPERTY, 
        TAG_COUNT_PROPERTY_DEFAULT));
    tagCardinality = new int[tagPairs];
    final String tagCardinalityString = p.getProperty(TAG_CARDINALITY_PROPERTY, 
        TAG_CARDINALITY_PROPERTY_DEFAULT);
    final String[] tagCardinalityParts = tagCardinalityString.split(",");
    int idx = 0;
    totalCardinality = numKeys;
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
      totalCardinality *= tagCardinality[idx];
      ++idx;
      if (idx >= tagPairs) {
        // we have more cardinalities than tag keys so bail at this point.
        break;
      }
    }
    if (numKeys < threads) {
      throw new WorkloadException("Field count " + numKeys + " (keys for time "
          + "series workloads) must be greater or equal to the number of "
          + "threads " + threads);
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
    
    keys = new String[numKeys];
    for (int i = 0; i < numKeys; ++i) {
      keys[i] = keyGenerator.nextString();
    }
    if (randomizeTimeseriesOrder) {
      Utils.shuffleArray(keys);
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
    if (randomizeTimeseriesOrder) {
      for (int i = 0; i < tagValues.length; i++) {
        Utils.shuffleArray(tagValues[i]);
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
    
    dataintegrity = Boolean.parseBoolean(
        p.getProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY, 
            CoreWorkload.DATA_INTEGRITY_PROPERTY_DEFAULT));
    
    queryTimeSpan = Integer.parseInt(p.getProperty(QUERY_TIMESPAN_PROPERTY, 
        QUERY_TIMESPAN_PROPERTY_DEFAULT));
    queryTimeSpanDelimiter = p.getProperty(QUERY_TIMESPAN_DELIMITER_PROPERTY, 
        QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
    
    groupByFunction = p.getProperty(GROUPBY_PROPERTY);
    if (groupByFunction != null && !groupByFunction.isEmpty()) {
      final String groupByKeys = p.getProperty(GROUPBY_KEYS_PROPERTY);
      if (groupByKeys == null || groupByKeys.isEmpty()) {
        throw new WorkloadException("Group by was enabled but no keys were specified.");
      }
      final String[] gbKeys = groupByKeys.split(",");
      if (gbKeys.length != tagKeys.length) {
        throw new WorkloadException("Only " + gbKeys.length + " group by keys "
            + "were specified but there were " + tagKeys.length + " tag keys given.");
      }
      groupBys = new boolean[gbKeys.length];
      for (int i = 0; i < gbKeys.length; i++) {
        groupBys[i] = Integer.parseInt(gbKeys[i].trim()) == 0 ? false : true;
      }
      groupBy = true;
    }
    
    valueType = ValueType.fromString(p.getProperty(VALUE_TYPE_PROPERTY, VALUE_TYPE_PROPERTY_DEFAULT));
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
      if (groupBy && groupBys[i]) {
        fields.add(tagKeys[i]);
      } else {
        fields.add(tagKeys[i] + tagPairDelimiter + tagValues[i][Utils.random().nextInt(tagValues[i].length)]);
      }
    }
    // TODO - only query up to the time that data has been written and randomly pick
    // the timestamps to query.
    fields.add(TIMESTAMP_KEY + tagPairDelimiter + timestamp + queryTimeSpanDelimiter + (timestamp + queryTimeSpan));
    if (groupBy) {
      fields.add(groupByKey + tagPairDelimiter + groupByFunction);
    }
    if (downsample) {
      fields.add(downsampleKey + tagPairDelimiter + downsampleFunction);
    }
    
    final HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);
    
    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
    
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
  
  protected Status verifyRow(final String key, final HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.OK;
    long startTime = System.nanoTime();

    double value = 0;
    long timestamp = 0;
    final TreeMap<String, String> validationTags = new TreeMap<String, String>();
    for (final Entry<String, ByteIterator> entry : cells.entrySet()) {
      if (entry.getKey().equals(TIMESTAMP_KEY)) {
        final NumericByteIterator it = (NumericByteIterator) entry.getValue();
        timestamp = it.getLong();
      } else if (entry.getKey().equals(VALUE_KEY)) {
        final NumericByteIterator it = (NumericByteIterator) entry.getValue();
        value = it.getLong();
      } else {
        validationTags.put(entry.getKey(), entry.getValue().toString());
      }
    }

    if (validationFunction(key, timestamp, validationTags) != value) {
      verifyStatus = Status.UNEXPECTED_STATE;
    }
    long endTime = System.nanoTime();
    _measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
    _measurements.reportStatus("VERIFY", verifyStatus);
    return verifyStatus;
  }
  
  /**
   * Function used for generating a deterministic hash based on the combination
   * of metric, tags and timestamp.
   * @param key A non-null string representing the key.
   * @param timestamp A timestamp in the proper units for the workload.
   * @param tags A non-null map of tag keys and values NOT including the YCSB
   * key or timestamp.
   * @return A hash value as an 8 byte integer.
   */
  protected long validationFunction(final String key, final long timestamp, 
      final TreeMap<String, String> tags) {
    final StringBuilder validationBuffer = new StringBuilder(keys[0].length() + 
        (tagPairs * tagKeys[0].length()) + (tagPairs * tagValues[0].length));
    for (final Entry<String, String> pair : tags.entrySet()) {
      validationBuffer.append(pair.getKey()).append(pair.getValue());
    }
    return (long) validationBuffer.toString().hashCode() ^ timestamp;
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
    
    protected ThreadState(final int threadID, final int threadCount) throws WorkloadException {
      int totalThreads = threadCount > 0 ? threadCount : 1;
      
      if (threadID >= totalThreads) {
        throw new IllegalStateException("Thread ID " + threadID + " cannot be greater "
            + "than or equal than the thread count " + totalThreads);
      }
      if (keys.length < threadCount) {
        throw new WorkloadException("Thread count " + totalThreads + " must be greater "
            + "than or equal to key count " + keys.length);
      }
      
      int keysPerThread = keys.length / totalThreads;
      keyIdx = keyIdxStart = keysPerThread * threadID;
      if (totalThreads - 1 == threadID) {
        keyIdxEnd = keys.length;
      } else {
        keyIdxEnd = keyIdxStart + keysPerThread;
      }
      
      tagValueIdxs = new int[tagPairs]; // all zeros
      
      int intervals = 0;
      if (randomizeTimestampOrder) {
        intervals = (recordcount / (threadCount * keysPerThread * totalCardinality)) + 1;
      }
      
      final String startingTimestamp = 
          properties.getProperty(CoreWorkload.INSERT_START_PROPERTY);
      if (startingTimestamp == null || startingTimestamp.isEmpty()) {
        timestampGenerator = randomizeTimestampOrder ? 
            new RandomDiscreteTimestampGenerator(timestampInterval, timeUnits, intervals) :
            new UnixEpochTimestampGenerator(timestampInterval, timeUnits);
      } else {
        try {
          timestampGenerator = randomizeTimestampOrder ? 
              new RandomDiscreteTimestampGenerator(timestampInterval, timeUnits, Long.parseLong(startingTimestamp), intervals) :
              new UnixEpochTimestampGenerator(timestampInterval, timeUnits, Long.parseLong(startingTimestamp));
        } catch (NumberFormatException nfe) {
          throw new WorkloadException("Unable to parse the " + 
              CoreWorkload.INSERT_START_PROPERTY, nfe);
        }
      }
      // Set the last value properly for the timestamp, otherwise it may start 
      // one interval ago.
      timestampGenerator.nextValue();
      startTimestamp = timestampGenerator.lastValue();
      interval = timestampGenerator.getOffset(1);
      threadOps = recordcount / totalThreads;
      
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
      final TreeMap<String, String> validationTags;
      if (dataintegrity) {
        validationTags = new TreeMap<String, String>();
      } else {
        validationTags = null;
      }
      final String key = keys[keyIdx];
      for (int i = 0; i < tagPairs; ++i) {
        int tvidx = tagValueIdxs[i];
        map.put(tagKeys[i], new StringByteIterator(tagValues[i][tvidx]));
        if (dataintegrity) {
          validationTags.put(tagKeys[i], tagValues[i][tvidx]);
        }
      }
      
      map.put(TIMESTAMP_KEY, new NumericByteIterator(timestampGenerator.currentValue()));
      if (dataintegrity) {
        // TODO - value should be hash of keys
        map.put(VALUE_KEY, new NumericByteIterator(validationFunction(key, 
            timestampGenerator.currentValue(), validationTags)));
      } else {
        switch (valueType) {
        case INTEGERS:
          map.put(VALUE_KEY, new NumericByteIterator(Utils.random().nextInt()));
          break;
        case FLOATS:
          map.put(VALUE_KEY, new NumericByteIterator(
              Utils.random().nextDouble() * (double) 100000));
        case MIXED:
          if (Utils.random().nextBoolean()) {
            map.put(VALUE_KEY, new NumericByteIterator(Utils.random().nextInt()));
          } else {
            map.put(VALUE_KEY, new NumericByteIterator(
                Utils.random().nextDouble() * (double) 100000));
          }
          break;
        default:
          throw new IllegalStateException("Somehow we didn't have a value type configured that we support: " + valueType);
        }        
      }
      
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
