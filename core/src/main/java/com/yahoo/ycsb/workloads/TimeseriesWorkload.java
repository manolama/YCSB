package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.RandomPrintableStringGenerator;

public class TimeseriesWorkload extends Workload {

  private RandomPrintableStringGenerator keyGenerator;
  private RandomPrintableStringGenerator tagKeyGenerator;
  private RandomPrintableStringGenerator tagValueGenerator;
  private int recordcount;
  private int tagPairs;
  private String table;
  
  @Override
  public void init(Properties p) throws WorkloadException {
    recordcount =
        Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, 
            Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    
    keyGenerator = new RandomPrintableStringGenerator(64);
    tagKeyGenerator = new RandomPrintableStringGenerator(8);
    tagValueGenerator = new RandomPrintableStringGenerator(16);
    tagPairs = 4;
  }
  
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    final String key = keyGenerator.nextValue();
    final Map<String, String> tags = new HashMap<String, String>();
    for (int i = 0; i < tagPairs; i++) {
      tags.put(tagKeyGenerator.nextValue(), tagValueGenerator.nextValue());
    }
    if (db.insert(table, key, StringByteIterator.getByteIteratorMap(tags)) == Status.OK) {
      return true;
    }
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    // TODO Auto-generated method stub
    return false;
  }

}
