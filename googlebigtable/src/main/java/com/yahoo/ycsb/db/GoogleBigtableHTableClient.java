package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map.Entry;

import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Preconditions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class GoogleBigtableHTableClient extends com.yahoo.ycsb.DB {
  private Configuration config = HBaseConfiguration.create();
  private Connection connection = null;
  
  private boolean debug = false;

  private String tableName = "";
  
  //Depending on the value of clientSideBuffering, either bufferedMutator
  // (clientSideBuffering) or currentTable (!clientSideBuffering) will be used.
  private Table currentTable = null;
  private BufferedMutator bufferedMutator = null;

  private String columnFamily = "cf";
  private byte[] columnFamilyBytes = "cf".getBytes();
 
  /**
   * Durability to use for puts and deletes.
   */
  private Durability durability = Durability.USE_DEFAULT;

  /** Whether or not a page filter should be used to limit scan length. */
  private boolean usePageFilter = true;

  /**
   * If true, buffer mutations on the client. This is the default behavior for
   * HBaseClient. For measuring insert/update/delete latencies, client side
   * buffering should be disabled.
   */
  private boolean clientSideBuffering = false;
  private long writeBufferSize = 1024 * 1024 * 12;
  
  @Override
  public void init() throws DBException {
    
    Properties props = getProperties();
    
    props.put("google.bigtable.project.id", "calcium-post-108621");
    props.put("google.bigtable.cluster.name", "opentsdb");
    props.put("google.bigtable.zone.name", "us-central1-c");
    props.put("google.bigtable.auth.service.account.enable", "true");
    props.put("google.bigtable.auth.json.keyfile", "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json");
    props.put("google.bigtable.grpc.channel.count", "4");
    
    // htable
    props.put("hbase.client.connection.impl", "com.google.cloud.bigtable.hbase1_0.BigtableConnection");
    props.put("hbase.hconnection.threads.max", "16");
    
    Iterator<Entry<Object, Object>> it = props.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Object, Object> entry = it.next();
      config.set((String)entry.getKey(), (String)entry.getValue());
    }
    
    try {
      connection = ConnectionFactory.createConnection(config);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
 // if this is a "new" table, init HTable object. Else, use existing one
    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        getHTable(table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    Put p = new Put(Bytes.toBytes(key));
    p.setDurability(durability);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] value = entry.getValue().toArray();
      if (debug) {
        System.out.println("Adding field/value " + entry.getKey() + "/"
            + Bytes.toStringBinary(value) + " to put request");
      }
      p.addColumn(columnFamilyBytes, Bytes.toBytes(entry.getKey()), value);
    }

    try {
      if (clientSideBuffering) {
        Preconditions.checkNotNull(bufferedMutator);
        bufferedMutator.mutate(p);
      } else {
        currentTable.put(p);
      }
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing put: " + e);
      }
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      // do nothing for now...hope this is rare
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    // TODO Auto-generated method stub
    return null;
  }

  public void getHTable(String table) throws IOException {
    final TableName tName = TableName.valueOf(table);
    this.currentTable = this.connection.getTable(tName);
    // suggestions from
    // http://ryantwopointoh.blogspot.com/2009/01/
    // performance-of-hbase-importing.html
    if (clientSideBuffering) {
      final BufferedMutatorParams p = new BufferedMutatorParams(tName);
      p.writeBufferSize(writeBufferSize);
      this.bufferedMutator = this.connection.getBufferedMutator(p);
    }
  }
}
