package com.yahoo.ycsb.workloads;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.Vector;
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
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IncrementingPrintableStringGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.RandomDiscreteTimestampGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SequentialGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

public class TimeSeriesWorkload extends Workload {  
  
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
  public static final String TIMESTAMP_INTERVAL_PROPERTY = "timestampinterval";    
  public static final String TIMESTAMP_INTERVAL_PROPERTY_DEFAULT = "60";    
      
  /** Name and default value for the timestamp units property. */   
  public static final String TIMESTAMP_UNITS_PROPERTY = "timestampunits";    
  public static final String TIMESTAMP_UNITS_PROPERTY_DEFAULT = "SECONDS"; 
  
  public static final String TAG_COUNT_PROPERTY = "tagcount";
  public static final String TAG_COUNT_PROPERTY_DEFAULT = "4";
  
  public static final String TAG_CARDINALITY_PROPERTY = "tagcardinality";
  public static final String TAG_CARDINALITY_PROPERTY_DEFAULT = "1, 2, 4, 8";
  
  public static final String TAG_KEY_LENGTH_PROPERTY = "tagkeylength";
  public static final String TAG_KEY_LENGTH_PROPERTY_DEFAULT = "8";
  
  public static final String TAG_VALUE_LENGTH_PROPERTY = "tagvaluelength";
  public static final String TAG_VALUE_LENGTH_PROPERTY_DEFAULT = "8";
  
  public static final String PAIR_DELIMITER_PROPERTY = "tagpairdelimiter";
  public static final String PAIR_DELIMITER_PROPERTY_DEFAULT = "=";
  
  public static final String DELETE_DELIMITER_PROPERTY = "deletedelimiter";
  public static final String DELETE_DELIMITER_PROPERTY_DEFAULT = ":";
  
  public static final String RANDOMIZE_TIMESTAMP_ORDER_PROPERTY = "randomwritetimestamporder";
  public static final String RANDOMIZE_TIMESTAMP_ORDER_PROPERTY_DEFAULT = "false";
  
  public static final String RANDOMIZE_TIMESERIES_ORDER_PROPERTY = "randomtimeseriesorder";
  public static final String RANDOMIZE_TIMESERIES_ORDER_PROPERTY_DEFAULT = "false";
  
  public static final String VALUE_TYPE_PROPERTY = "valuetype";
  public static final String VALUE_TYPE_PROPERTY_DEFAULT = "integers";
  
  public static final String SPARSITY_PROPERTY = "sparsity";
  public static final String SPARSITY_PROPERTY_DEFAULT = "0.00";
  
  // Query params
  public static final String QUERY_TIMESPAN_PROPERTY = "querytimespan";
  public static final String QUERY_TIMESPAN_PROPERTY_DEFAULT = "0";
  
  public static final String QUERY_RANDOM_TIMESPAN_PROPERTY = "queryrandomtimespan";
  public static final String QUERY_RANDOM_TIMESPAN_PROPERTY_DEFAULT = "false";
  
  public static final String QUERY_TIMESPAN_DELIMITER_PROPERTY = "querytimespandelimiter";
  public static final String QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT = ",";
  
  public static final String GROUPBY_KEY_PROPERTY = "groupbykey";
  public static final String GROUPBY_KEY_PROPERTY_DEFAULT = "YCSBGB";
  
  public static final String GROUPBY_PROPERTY = "groupbyfunction";
  
  public static final String GROUPBY_KEYS_PROPERTY = "groupbykeys";
  
  public static final String DOWNSAMPLING_KEY_PROPERTY = "downsamplingkey";
  public static final String DOWNSAMPLING_KEY_PROPERTY_DEFAULT = "YCSBDS";
  
  public static final String DOWNSAMPLING_FUNCTION_PROPERTY = "downsamplingfunction";
  public static final String DOWNSAMPLING_INTERVAL_PROPERTY = "downsamplinginterval";
  
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
  private int seriesCardinality;
  protected NumberGenerator scanlength;
  protected NumberGenerator keychooser;
  protected DiscreteGenerator operationchooser;
  protected int maxOffsets;
  
  private int recordcount;
  private int tagPairs;
  private String table;
  
  private String[] keys;

  private int numKeys;
  private String[] tagKeys;
  private int[] tagCardinality;
  private String[] tagValues;
  private int firstIncrementableCardinality;
  private double sparsity;
  
  // Query parameters
  private int queryTimeSpan;
  private boolean queryRandomTimeSpan;
  private String tagPairDelimiter;
  private String deleteDelimiter;
  private String queryTimeSpanDelimiter;
  private boolean groupBy;
  private String groupByKey;
  private String groupByFunction;
  private boolean[] groupBys;
  private boolean downsample;
  private String downsampleKey;
  private String downsampleFunction;
  private int downsampleInterval;

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
    
    operationchooser = CoreWorkload.createOperationGenerator(properties);
    
    int maxscanlength =
        Integer.parseInt(p.getProperty(CoreWorkload.MAX_SCAN_LENGTH_PROPERTY, 
            CoreWorkload.MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanlengthdistrib =
        p.getProperty(CoreWorkload.SCAN_LENGTH_DISTRIBUTION_PROPERTY, 
            CoreWorkload.SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    
    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformIntegerGenerator(1, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(1, maxscanlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }
    
    randomizeTimestampOrder = Boolean.parseBoolean(p.getProperty(
        RANDOMIZE_TIMESTAMP_ORDER_PROPERTY, 
        RANDOMIZE_TIMESTAMP_ORDER_PROPERTY_DEFAULT));
    randomizeTimeseriesOrder = Boolean.parseBoolean(p.getProperty(
        RANDOMIZE_TIMESERIES_ORDER_PROPERTY, 
        RANDOMIZE_TIMESERIES_ORDER_PROPERTY_DEFAULT));
    
    // setup the key, tag key and tag value generators
    final int keyLength = Integer.parseInt(p.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY, 
        CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT));
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
    sparsity = Double.parseDouble(p.getProperty(SPARSITY_PROPERTY, SPARSITY_PROPERTY_DEFAULT));
    tagCardinality = new int[tagPairs];
    final String tagCardinalityString = p.getProperty(TAG_CARDINALITY_PROPERTY, 
        TAG_CARDINALITY_PROPERTY_DEFAULT);
    final String[] tagCardinalityParts = tagCardinalityString.split(",");
    int idx = 0;
    totalCardinality = numKeys;
    seriesCardinality = 1;
    int maxCardinality = 0;
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
      seriesCardinality *= tagCardinality[idx];
      if (tagCardinality[idx] > maxCardinality) {
        maxCardinality = tagCardinality[idx];
      }
      ++idx;
      if (idx >= tagPairs) {
        // we have more cardinalities than tag keys so bail at this point.
        break;
      }
    }
    System.out.println("TOTAL CARD: " + totalCardinality);
    System.out.println("SERIES CARD: " + seriesCardinality);
    System.out.println("MAX CARD: " + maxCardinality);
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
    tagKeys = new String[tagPairs];
    tagValues = new String[maxCardinality];
    for (int i = 0; i < numKeys; ++i) {
      keys[i] = keyGenerator.nextString();
    }

    for (int i = 0; i < tagPairs; ++i) {
      tagKeys[i] = tagKeyGenerator.nextString();
    }
    
    for (int i = 0; i < maxCardinality; i++) {
      tagValues[i] = tagValueGenerator.nextString();
    }
    if (randomizeTimeseriesOrder) {
      Utils.shuffleArray(keys);
      Utils.shuffleArray(tagKeys);
      Utils.shuffleArray(tagValues);
    }
    
    String requestdistrib =
        p.getProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, 
            CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformIntegerGenerator(0, keys.length - 1);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(0, keys.length- 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      keychooser = new ScrambledZipfianGenerator(0, keys.length- 1);
    //} else if (requestdistrib.compareTo("latest") == 0) {
      //keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(CoreWorkload.HOTSPOT_DATA_FRACTION, CoreWorkload.HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(CoreWorkload.HOTSPOT_OPN_FRACTION, CoreWorkload.HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(0, keys.length- 1,
          hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
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
    
    maxOffsets = (recordcount / totalCardinality) + 1;
    
    tagPairDelimiter = p.getProperty(PAIR_DELIMITER_PROPERTY, PAIR_DELIMITER_PROPERTY_DEFAULT);
    deleteDelimiter = p.getProperty(DELETE_DELIMITER_PROPERTY, DELETE_DELIMITER_PROPERTY_DEFAULT);
    dataintegrity = Boolean.parseBoolean(
        p.getProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY, 
            CoreWorkload.DATA_INTEGRITY_PROPERTY_DEFAULT));
    
    queryTimeSpan = Integer.parseInt(p.getProperty(QUERY_TIMESPAN_PROPERTY, 
        QUERY_TIMESPAN_PROPERTY_DEFAULT));
    queryRandomTimeSpan = Boolean.parseBoolean(p.getProperty(QUERY_RANDOM_TIMESPAN_PROPERTY, 
        QUERY_RANDOM_TIMESPAN_PROPERTY_DEFAULT));
    queryTimeSpanDelimiter = p.getProperty(QUERY_TIMESPAN_DELIMITER_PROPERTY, 
        QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
    
    groupByKey = p.getProperty(GROUPBY_KEY_PROPERTY, GROUPBY_KEY_PROPERTY_DEFAULT);
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
    
    downsampleKey = p.getProperty(DOWNSAMPLING_KEY_PROPERTY, DOWNSAMPLING_KEY_PROPERTY_DEFAULT);
    downsampleFunction = p.getProperty(DOWNSAMPLING_FUNCTION_PROPERTY);
    if (downsampleFunction != null && !downsampleFunction.isEmpty()) {
      downsampleInterval = Integer.parseInt(p.getProperty(DOWNSAMPLING_INTERVAL_PROPERTY));
      downsample = true;
    }
    
    valueType = ValueType.fromString(p.getProperty(VALUE_TYPE_PROPERTY, VALUE_TYPE_PROPERTY_DEFAULT));
    
    table = p.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
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
      throw new IllegalStateException("Missing thread state.");
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
      throw new IllegalStateException("Missing thread state.");
    }
    switch (operationchooser.nextString()) {
    case "READ":
      doTransactionRead(db, threadstate);
      break;
    case "UPDATE":
      doTransactionUpdate(db, threadstate);
      break;
    case "INSERT": 
      doTransactionInsert(db, threadstate);
      break;
    case "SCAN":
      doTransactionScan(db, threadstate);
      break;
    case "DELETE":
      doTransactionDelete(db, threadstate);
      break;
    default:
      return false;
    }
    return true;
  }

  protected void doTransactionRead(final DB db, Object threadstate) {
    final ThreadState state = (ThreadState) threadstate;
    final String keyname = keys[keychooser.nextValue().intValue()];
    
    int offsets = state.queryOffsetGenerator.nextValue().intValue();
    //int offsets = Utils.random().nextInt(maxOffsets - 1);
    final long startTimestamp;
    if (offsets > 0) {
      startTimestamp = state.startTimestamp + state.timestampGenerator.getOffset(offsets);
    } else {
      startTimestamp = state.startTimestamp;
    }
    
    // rando tags
    HashSet<String> fields = new HashSet<String>();
    for (int i = 0; i < tagPairs; ++i) {
      if (groupBy && groupBys[i]) {
        fields.add(tagKeys[i]);
      } else {
        fields.add(tagKeys[i] + tagPairDelimiter + 
            tagValues[Utils.random().nextInt(tagCardinality[i])]);
      }
    }
    
    if (queryTimeSpan > 0) {
      final long endTimestamp;
      if (queryRandomTimeSpan) {
        endTimestamp = startTimestamp + (timestampInterval * Utils.random().nextInt(queryTimeSpan / timestampInterval));
      } else {
        endTimestamp = startTimestamp + queryTimeSpan;
      }
      fields.add(TIMESTAMP_KEY + tagPairDelimiter + startTimestamp + queryTimeSpanDelimiter + endTimestamp);
    } else {
      fields.add(TIMESTAMP_KEY + tagPairDelimiter + startTimestamp);  
    }
    if (groupBy) {
      fields.add(groupByKey + tagPairDelimiter + groupByFunction);
    }
    if (downsample) {
      fields.add(downsampleKey + tagPairDelimiter + downsampleFunction + tagPairDelimiter + downsampleInterval);
    }
    
    final HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    final Status status = db.read(table, keyname, fields, cells);
    
    if (dataintegrity && status == Status.OK) {
      verifyRow(keyname, cells);
    }
  }
  
  protected void doTransactionUpdate(final DB db, Object threadstate) {
    if (threadstate == null) {
      throw new IllegalStateException("Missing thread state.");
    }
    final HashMap<String, ByteIterator> tags = new HashMap<String, ByteIterator>(tagPairs);
    final String key = ((ThreadState)threadstate).nextDataPoint(tags);
    db.update(table, key, tags);
  }
  
  protected void doTransactionInsert(final DB db, Object threadstate) {
    doInsert(db, threadstate);
  }
  
  protected void doTransactionScan(final DB db, Object threadstate) {
    final ThreadState state = (ThreadState) threadstate;
    
    final String keyname = keys[Utils.random().nextInt(keys.length)];
    
    // choose a random scan length
    int len = scanlength.nextValue().intValue();
    
    int offsets = Utils.random().nextInt(maxOffsets - 1);
    final long startTimestamp;
    if (offsets > 0) {
      startTimestamp = state.startTimestamp + state.timestampGenerator.getOffset(offsets);
    } else {
      startTimestamp = state.startTimestamp;
    }
    
    // rando tags
    HashSet<String> fields = new HashSet<String>();
    for (int i = 0; i < tagPairs; ++i) {
      if (groupBy && groupBys[i]) {
        fields.add(tagKeys[i]);
      } else {
        fields.add(tagKeys[i] + tagPairDelimiter + 
            tagValues[Utils.random().nextInt(tagCardinality[i])]);
      }
    }
    
    if (queryTimeSpan > 0) {
      final long endTimestamp;
      if (queryRandomTimeSpan) {
        endTimestamp = startTimestamp + (timestampInterval * Utils.random().nextInt(queryTimeSpan / timestampInterval));
      } else {
        endTimestamp = startTimestamp + queryTimeSpan;
      }
      fields.add(TIMESTAMP_KEY + tagPairDelimiter + startTimestamp + queryTimeSpanDelimiter + endTimestamp);
    } else {
      fields.add(TIMESTAMP_KEY + tagPairDelimiter + startTimestamp);  
    }
    if (groupBy) {
      fields.add(groupByKey + tagPairDelimiter + groupByFunction);
    }
    if (downsample) {
      fields.add(downsampleKey + tagPairDelimiter + downsampleFunction + tagPairDelimiter + downsampleInterval);
    }
    
    final Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
    final Status status = db.scan(table, keyname, len, fields, results);
    
//    if (dataintegrity && status == Status.OK) {
//      verifyRow(keyname, cells);
//    }
  }
  
  protected void doTransactionDelete(final DB db, Object threadstate) {
    final ThreadState state = (ThreadState) threadstate;
    
    final StringBuilder buf = new StringBuilder().append(keys[Utils.random().nextInt(keys.length)]);
    
    int offsets = Utils.random().nextInt(maxOffsets - 1);
    final long startTimestamp;
    if (offsets > 0) {
      startTimestamp = state.startTimestamp + state.timestampGenerator.getOffset(offsets);
    } else {
      startTimestamp = state.startTimestamp;
    }
    
    // rando tags
    for (int i = 0; i < tagPairs; ++i) {
      if (groupBy && groupBys[i]) {
        buf.append(deleteDelimiter)
           .append(tagKeys[i]);
      } else {
        buf.append(deleteDelimiter)
           .append(tagKeys[i] + tagPairDelimiter + 
               tagValues[Utils.random().nextInt(tagCardinality[i])]);
      }
    }
    
    if (queryTimeSpan > 0) {
      final long endTimestamp;
      if (queryRandomTimeSpan) {
        endTimestamp = startTimestamp + (timestampInterval * Utils.random().nextInt(queryTimeSpan / timestampInterval));
      } else {
        endTimestamp = startTimestamp + queryTimeSpan;
      }
      buf.append(deleteDelimiter)
         .append(TIMESTAMP_KEY + tagPairDelimiter + startTimestamp + queryTimeSpanDelimiter + endTimestamp);
    } else {
      buf.append(deleteDelimiter)
         .append(TIMESTAMP_KEY + tagPairDelimiter + startTimestamp);  
    }
    
    db.delete(table, buf.toString());
  }
  
  protected Status verifyRow(final String key, final HashMap<String, ByteIterator> cells) {
    Status verifyStatus = Status.UNEXPECTED_STATE;
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
        value = it.isFloatingPoint() ? it.getDouble() : it.getLong();
      } else {
        validationTags.put(entry.getKey(), entry.getValue().toString());
      }
    }

    if (validationFunction(key, timestamp, validationTags) == value) {
      verifyStatus = Status.OK;
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
        (tagPairs * tagKeys[0].length()) + (tagPairs * tagCardinality[0]));
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
    protected final NumberGenerator queryOffsetGenerator;
    
    private int keyIdx;
    private int keyIdxStart;
    private int keyIdxEnd;
    private int[] tagValueIdxs;

    private boolean rollover;
    long startTimestamp;
    
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
      
      final String startingTimestamp = 
          properties.getProperty(CoreWorkload.INSERT_START_PROPERTY);
      if (startingTimestamp == null || startingTimestamp.isEmpty()) {
        timestampGenerator = randomizeTimestampOrder ? 
            new RandomDiscreteTimestampGenerator(timestampInterval, timeUnits, maxOffsets) :
            new UnixEpochTimestampGenerator(timestampInterval, timeUnits);
      } else {
        try {
          timestampGenerator = randomizeTimestampOrder ? 
              new RandomDiscreteTimestampGenerator(timestampInterval, timeUnits, Long.parseLong(startingTimestamp), maxOffsets) :
              new UnixEpochTimestampGenerator(timestampInterval, timeUnits, Long.parseLong(startingTimestamp));
        } catch (NumberFormatException nfe) {
          throw new WorkloadException("Unable to parse the " + 
              CoreWorkload.INSERT_START_PROPERTY, nfe);
        }
      }
      // Set the last value properly for the timestamp, otherwise it may start 
      // one interval ago.
      startTimestamp = timestampGenerator.nextValue();
      // TODO - pick it
      queryOffsetGenerator = new UniformIntegerGenerator(0, maxOffsets - 2);
    }
    
    private String nextDataPoint(HashMap<String, ByteIterator> map) {
      int iterations = sparsity <= 0 ? 1 : Utils.random().nextInt((int) ((double) seriesCardinality * sparsity));
      if (iterations < 1) {
        iterations = 1;
      }
      while (true) {
        iterations--;
        if (rollover) {
          timestampGenerator.nextValue();
          rollover = false;
        }
        String key = null;
        if (iterations <= 0) {
          final TreeMap<String, String> validationTags;
          if (dataintegrity) {
            validationTags = new TreeMap<String, String>();
          } else {
            validationTags = null;
          }
          key = keys[keyIdx];
          for (int i = 0; i < tagPairs; ++i) {
            int tvidx = tagValueIdxs[i];
            map.put(tagKeys[i], new StringByteIterator(tagValues[tvidx]));
            if (dataintegrity) {
              validationTags.put(tagKeys[i], tagValues[tvidx]);
            }
          }
          
          map.put(TIMESTAMP_KEY, new NumericByteIterator(timestampGenerator.currentValue()));
          if (dataintegrity) {
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
        
        if (iterations <= 0) {
          return key;
        }
      }
    }
  }

}
