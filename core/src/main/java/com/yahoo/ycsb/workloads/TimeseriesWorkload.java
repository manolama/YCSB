package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Properties;

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

public class TimeseriesWorkload extends Workload {

  public static final String TAG_COUNT_PROPERTY = "tagcount";
  public static final String TAG_COUNT_PROPERTY_DEFAULT = "4";
  
  private Generator<String> keyGenerator;
  private Generator<String> tagKeyGenerator;
  private Generator<String> tagValueGenerator;
  private int recordcount;
  private int tagPairs;
  private String table;
  private long timestamp;
  
  private String[] keys;
  private int keyIdx;
  private int numKeys;
  private String[] tagKeys;
  private int[] tagCardinality;
  private String[][] tagValues;
  private int[] tagValueIdxs;
  private int msbTagCardinality;
  private boolean rollover;
  
  @Override
  public void init(Properties p) throws WorkloadException {
    timestamp = System.currentTimeMillis() / 1000;
    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, 
            Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    
    keyGenerator = new OrderedPrintableStringGenerator(4);
    tagKeyGenerator = new OrderedPrintableStringGenerator(2);
    tagValueGenerator = new OrderedPrintableStringGenerator(4);
    tagPairs = 2;
    
    tagCardinality = new int[tagPairs];
    tagCardinality[0] = 1;
    tagCardinality[1] = 2;
//    tagCardinality[2] = 1;
//    tagCardinality[3] = 4;
    
    for (int i = 0; i < tagCardinality.length; ++i) {
      if (tagCardinality[i] > 1) {
        msbTagCardinality = i;
        break;
      }
    }
    numKeys = 2;
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

//    System.out.println("KEYS: " + Arrays.toString(keys));
//    System.out.println("TAG KEYS: " + Arrays.toString(tagKeys));
//    System.out.print("TAG VALUES: " );
//    for (int i = 0; i < tagValues.length; i++) {
//      if (i > 0) {
//        System.out.print(", ");
//      }
//      System.out.print("(" + i + ")" + Arrays.toString(tagValues[i]));
//    }
//    System.out.println();
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
      ++timestamp;
      rollover = false;
    }
    
    final String key = keys[keyIdx];
    for (int i = 0; i < tagPairs; ++i) {
      int tvidx = tagValueIdxs[i];
      map.put(tagKeys[i], new StringByteIterator(tagValues[i][tvidx]));
    }
    // TODO - byte array
    map.put("YCSBTS", new ByteArrayByteIterator(Utils.longToBytes(timestamp)));
    map.put("YCSBV", new ByteArrayByteIterator(Utils.doubleToBytes(
        Utils.random().nextDouble())));
    
    boolean tagRollover = false;
    for (int i = tagCardinality.length - 1; i >= 0; --i) {
      if (tagCardinality[i] <= 1) {
        // nothing to increment here
        continue;
      }
      
      if (tagValueIdxs[i] + 1 >= tagCardinality[i]) {
        tagValueIdxs[i] = 0;
        if (i == msbTagCardinality) {
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
