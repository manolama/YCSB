package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Utils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.utils.Config;

/**
 * OpenTSDB client.
 */
public class OpenTSDBClient20 extends com.yahoo.ycsb.DB {
  private static final Logger LOG = (Logger)LoggerFactory.getLogger(OpenTSDBClient20.class);
  
  private static final Object MUTEX = new Object();
  private static TSDB tsdbClient;
  private double value;
  private long timestamp;
  
  @Override
  public void init() throws DBException {
    Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
    
    try {
      synchronized (MUTEX) {
        if (tsdbClient == null) {
          final Config config = new Config("/etc/opentsdb/opentsdb.conf");
          tsdbClient = new TSDB(config);
        }
      }
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException("Failed instantiation", e);
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
    if (false) {
      System.out.println("Key: " + key);
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        System.out.println("  Field: " + entry.getKey() + "  Val: " + entry.getValue().toString());
      }
      return Status.OK;
    } else {
      final Map<String, String> tags = new HashMap<String, String>(values.size());
      int count = 0;
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        if (entry.getKey().equals("YCSBV")) {
          value = Utils.bytesToDouble(entry.getValue().toArray());
        } else if (entry.getKey().equals("YCSBTS")) {
          timestamp = Utils.bytesToLong(entry.getValue().toArray());
        } else {
          tags.put(entry.getKey(), new String(entry.getValue().toString()));
        }
      }
      try {
        WritableDataPoints wdps = tsdbClient.newDataPoints();
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
  public Status read(String arg0, String arg1, Set<String> arg2,
      HashMap<String, ByteIterator> arg3) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status scan(String arg0, String arg1, int arg2, Set<String> arg3,
      Vector<HashMap<String, ByteIterator>> arg4) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    // TODO Auto-generated method stub
    return null;
  }

}
