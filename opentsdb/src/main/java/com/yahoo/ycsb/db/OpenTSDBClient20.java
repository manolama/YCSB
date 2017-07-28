package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.Vector;

import com.google.common.collect.Lists;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.workloads.TimeseriesWorkload;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.Config;

/**
 * OpenTSDB client.
 */
public class OpenTSDBClient20 extends com.yahoo.ycsb.DB {
  private static final Logger LOG = (Logger)LoggerFactory.getLogger(OpenTSDBClient20.class);
  
  private static final Object MUTEX = new Object();
  private static int COUNTER = 0;
  private static TSDB TSDB_CLIENT;
  
  private int threadId = 0;
  private int timestampInterval;
  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  private String groupByKey;
  private String downsampleKey;
  
  @Override
  public void init() throws DBException {
    Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
    
    try {
      synchronized (MUTEX) {
        if (COUNTER == 0) {
          this.getProperties().getProperty("opentsdbConfig");
          final Config config = new Config("/etc/opentsdb/os_opentsdb.conf");
          TSDB_CLIENT = new TSDB(config);
          LOG.info("Successfully initialized TSDB client on thread: " + threadId);
          System.out.println("Successfully initialized TSDB client on thread: " + threadId);
        }
        threadId = COUNTER++;
      }
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException("Failed instantiation", e);
    }
    
    timestampInterval = Integer.parseInt(getProperties().getProperty(
        TimeseriesWorkload.TIMESTAMP_INTERVAL_PROPERTY, 
        TimeseriesWorkload.TIMESTAMP_INTERVAL_PROPERTY_DEFAULT));
    tagPairDelimiter = getProperties().getProperty(
        TimeseriesWorkload.PAIR_DELIMITER_PROPERTY, 
        TimeseriesWorkload.PAIR_DELIMITER_PROPERTY_DEFAULT);
    queryTimeSpanDelimiter = getProperties().getProperty(
        TimeseriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY,
        TimeseriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
    groupByKey = getProperties().getProperty(
        TimeseriesWorkload.GROUPBY_KEY_PROPERTY,
        TimeseriesWorkload.GROUPBY_KEY_PROPERTY_DEFAULT);
    downsampleKey = getProperties().getProperty(
        TimeseriesWorkload.DOWNSAMPLING_KEY_PROPERTY,
        TimeseriesWorkload.DOWNSAMPLING_KEY_PROPERTY_DEFAULT);
  }
  
  @Override
  public void cleanup() throws DBException {
    try {
      synchronized (MUTEX) {
        int countDown = --COUNTER;
        if (countDown < 1) {
          TSDB_CLIENT.shutdown().join();
          TSDB_CLIENT = null;
          LOG.info("Successfully shutdown TSDB client on thread: " + threadId);
          System.out.println("Successfully shutdown TSDB client on thread: " + threadId);
        }
      }
      
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed cleanup", e);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  @Override
  public Status delete(String arg0, String arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    long longVal = 0;
    double doubleVal = 0;
    long timestamp = 0;
    boolean isFloat = false;
    if (false) {
      System.out.println("Key: " + key);
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        
        if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
          final NumericByteIterator it = (NumericByteIterator) entry.getValue();
          System.out.println("  Field: " + entry.getKey() + "  Val: " + 
          (it.isFloatingPoint() ? it.getDouble() : it.getLong()));
        } else if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
          final NumericByteIterator it = (NumericByteIterator) entry.getValue();
          System.out.println("  Field: " + entry.getKey() + "  Val: " + it.getLong());
        } else {
          //System.out.println("  Field: " + entry.getKey() + "  Val: " + entry.getValue().toString());
        }
      }
      return Status.OK;
    } else {
      final Map<String, String> tags = new HashMap<String, String>(values.size());
      int count = 0;
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
          final NumericByteIterator it = (NumericByteIterator) entry.getValue();
          if (isFloat = it.isFloatingPoint()) {
            doubleVal = it.getDouble();
          } else {
            longVal = it.getLong();
          }
        } else if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
          timestamp = Utils.bytesToLong(entry.getValue().toArray());
        } else {
          tags.put(entry.getKey(), new String(entry.getValue().toString()));
        }
      }
      try {
        WritableDataPoints wdps = TSDB_CLIENT.newDataPoints();
        wdps.setBatchImport(false);
        wdps.setBufferingTime((short)0);
        wdps.setSeries(key, tags);
        wdps.addPoint(System.currentTimeMillis(), 1).join(50000);
        return Status.OK;
      } catch (Exception e) {
        LOG.error("WTF?", e);
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status read(final String table, 
                     final String keyname, 
                     final Set<String> fields,
                     final HashMap<String, ByteIterator> results) {
    final TSQuery query = new TSQuery();
    final TSSubQuery subQuery = new TSSubQuery();
    subQuery.setMetric(keyname);
    subQuery.setAggregator("sum");
    
    final List<TagVFilter> filters = Lists.newArrayList();
    for (final String field : fields) {
      final String[] pair = field.split(tagPairDelimiter);
      if (pair[0].equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
        final String[] range = pair[1].split(queryTimeSpanDelimiter);
        if (range.length == 1) {
          query.setStart(range[0]);
          query.setEnd(Long.toString(Long.parseLong(range[0]) + 
              (timestampInterval - 1 > 0 ? timestampInterval - 1 : 1)));
        } else {
          query.setStart(range[0]);
          query.setEnd(range[1]);
        }
      } else if (pair[0].equals(groupByKey)) {
        subQuery.setAggregator(pair[1]);
      } else if (pair[0].equals(downsampleKey)) {
        // TODO
        //subQuery.setDownsample(downsample);
      } else {
        if (pair.length == 2) {
          filters.add(TagVFilter.Builder()
              .setTagk(pair[0])
              .setFilter(pair[1])
              .setType("literal_or")
              .build());
        } else {
          filters.add(TagVFilter.Builder()
              .setTagk(pair[0])
              .setFilter("*")
              .setType("wildcard")
              .setGroupBy(true)
              .build());
        }
      }
    }
    query.setQueries((ArrayList<TSSubQuery>) Lists.newArrayList(subQuery)); 
    query.validateAndSetQuery();
    
    // TODO async
    try {
      final Query[] response = query.buildQueries(TSDB_CLIENT);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String arg0, String arg1, int arg2, Set<String> arg3,
      Vector<HashMap<String, ByteIterator>> arg4) {
    // TODO Auto-generated method stub
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    return insert(table, key, values);
  }

}
