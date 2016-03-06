package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.hbase.async.Config;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class AsyncHBaseClient extends com.yahoo.ycsb.DB {
  private static final Object MUTEX = new Object();
  private static HBaseClient client;
  
  private byte[] columnFamilyBytes = "cf".getBytes();
  
  private String lastTable = "";
  private byte[] lastTableBytes;
  
  private long joinTimeout = 30000;
  
  /**
   * Durability to use for puts and deletes.
   */
  private boolean durability = true;
  
  /**
   * If true, buffer mutations on the client. This is the default behavior for
   * HBaseClient. For measuring insert/update/delete latencies, client side
   * buffering should be disabled.
   */
  private boolean clientSideBuffering = false;
  
  @Override
  public void init() throws DBException {
    Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
    
    try {
      synchronized (MUTEX) {
        if (client == null) {
          final Config config = new Config("/etc/opentsdb/opentsdb.conf");
          client = new HBaseClient(config);
        }
      }
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException("Failed instantiation", e);
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    setTable(table);
    
    final GetRequest get = new GetRequest(
        lastTableBytes, key.getBytes(), columnFamilyBytes);
    if (fields != null) {
      get.qualifiers(getQualifierList(fields));
    }
    
    try {
      final ArrayList<KeyValue> row = client.get(get).join(joinTimeout);
      if (row == null || row.isEmpty()) {
        return Status.NOT_FOUND;
      }
      
      // got something so populate the results
      for (final KeyValue column : row) {
        result.put(new String(column.qualifier()), 
            // TODO - do we need to clone this array? YCSB may keep it in memory
            // for a while which would mean the entire KV would hang out and won't
            // be GC'd.
            new ByteArrayByteIterator(column.value()));
      }
      return Status.OK;
    } catch (InterruptedException e) {
      System.err.println("Thread interrupted");
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      System.err.println("Failure reading from row with key " + key + 
          ": " + e.getMessage());
      return Status.ERROR;
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    setTable(table);
    
    final Scanner scanner = client.newScanner(lastTableBytes);
    scanner.setFamily(columnFamilyBytes);
    scanner.setStartKey(startkey);
    // No end key... *sniff*
    if (fields != null) {
      scanner.setQualifiers(getQualifierList(fields));
    }
    
    // no filters? *sniff*
    ArrayList<ArrayList<KeyValue>> rows = null;
    try {
      while ((rows = scanner.nextRows().join(joinTimeout)) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final HashMap<String, ByteIterator> rowResult =
              new HashMap<String, ByteIterator>(row.size());
          for (final KeyValue column : row) {
            rowResult.put(new String(column.qualifier()), 
                // TODO - do we need to clone this array? YCSB may keep it in memory
                // for a while which would mean the entire KV would hang out and won't
                // be GC'd.
                new ByteArrayByteIterator(column.value()));
          }
          result.add(rowResult);
        }
      }
      scanner.close().join(joinTimeout);
      return Status.OK;
    } catch (InterruptedException e) {
      System.err.println("Thread interrupted");
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      System.err.println("Failure reading from row with key " + startkey + 
          ": " + e.getMessage());
      return Status.ERROR;
    }
    
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    setTable(table);
    
    final byte[][] qualifiers = new byte[values.size()][];
    final byte[][] byteValues = new byte[values.size()][];
    
    int idx = 0;
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      qualifiers[idx] = entry.getKey().getBytes();
      byteValues[idx++] = entry.getValue().toArray();
    }
    
    final PutRequest put = new PutRequest(lastTableBytes, key.getBytes(), 
        columnFamilyBytes, qualifiers, byteValues);
    if (!durability) {
      put.setDurable(false);
    }
    if (!clientSideBuffering) {
      put.setBufferable(false);
      try {
        client.put(put).join(joinTimeout);
      } catch (InterruptedException e) {
        System.err.println("Thread interrupted");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        System.err.println("Failure reading from row with key " + key + 
            ": " + e.getMessage());
        return Status.ERROR;
      }
    } else {
      // hooray! Asynchronous write. But without a callback and an async
      // YCSB call we don't know whether it succeeded or not
      client.put(put);
    }
    
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    setTable(table);
    
    final DeleteRequest delete = new DeleteRequest(
        lastTableBytes, key.getBytes(), columnFamilyBytes);
    if (!durability) {
      delete.setDurable(false);
    }
    if (!clientSideBuffering) {
      delete.setBufferable(false);
      try {
        client.delete(delete).join(joinTimeout);
      } catch (InterruptedException e) {
        System.err.println("Thread interrupted");
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        System.err.println("Failure reading from row with key " + key + 
            ": " + e.getMessage());
        return Status.ERROR;
      }
    } else {
      // hooray! Asynchronous write. But without a callback and an async
      // YCSB call we don't know whether it succeeded or not
      client.delete(delete);
    }
    return Status.OK;
  }

  /**
   * Little helper to set the table byte array. If it's different than the last
   * table we reset the byte array. Otherwise we just use the existing array.
   * @param table The table we're operating against
   */
  private void setTable(final String table) {
    if (!lastTable.equals(table)) {
      lastTable = table;
      lastTableBytes = table.getBytes();
    }
  }
  
  /**
   * Little helper to build a qualifier byte array from a field set
   * @param fields The fields to fetch
   * @return The column qualifier byte arrays
   */
  private byte[][] getQualifierList(final Set<String> fields) {
    final byte[][] qualifiers = new byte[fields.size()][];
    int idx = 0;
    for (final String field : fields) {
      qualifiers[idx++] = field.getBytes();
    }
    return qualifiers;
  }
}
