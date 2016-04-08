/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.repackaged.com.google.protobuf.ServiceException;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowRange;
import com.google.bigtable.v1.Mutation.DeleteFromRow;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.bigtable.v1.RowFilter.Chain.Builder;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Preconditions;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * Google Bigtable client for YCSB framework.
 * 
 * Bigtable offers two APIs. These include a native GRPC API as well as an
 * HBase API wrapper for the GRPC API. This client implements both versions to
 * compare wrapping overhead.
 * 
 * Note that for the most part, the HBase API code should be kept in sync with 
 * HBaseClient10.java.
 */
public class GoogleBigtableClient extends com.yahoo.ycsb.DB {
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");
  
  private static final String ASYNC_MUTATOR_MAX_MEMORY = "mutatorMaxMemory";
  private static final String ASYNC_MAX_INFLIGHT_RPCS = "mutatorMaxInflightRPCs";
  private static final String USE_HBASE_API = "usehbaseapi";
  private static final String CLIENT_SIDE_BUFFERING = "clientbuffering";
  
  /** Must be an object for synchronization and tracking running thread counts. */ 
  private static Integer threadCount = 0;
  
  /** This will load the hbase-site.xml config file */
  private final static Configuration config = HBaseConfiguration.create();
  private static Connection connection = null;
  
  /** Depending on the value of clientSideBuffering, either bufferedMutator
   * (clientSideBuffering) or currentTable (!clientSideBuffering) will be used.*/
  private Table currentTable = null;
  private BufferedMutator bufferedMutator = null;
  
  /** Print debug information to standard out */
  private boolean debug = false;
  
  /** Global Bigtable native API objects, only instantiated when not using the 
   * HBase API */ 
  private static BigtableOptions options;
  private static BigtableSession session;
  
  /** Thread loacal Bigtable native API objects instantiated when not using the 
   * HBase API */
  private BigtableDataClient client;
  private HeapSizeManager heapSizeManager;
  private AsyncExecutor asyncExecutor;
  
  /** The column family use for the workload */
  private byte[] columnFamilyBytes;
  
  /** Cache for the last table name/ID to avoid recreating HTable instances 
   * or lookups. */
  private String lastTable = "";
  private byte[] lastTableBytes;
  
  /** Whether or not to use the HBase interface or Bigtable's native interface */
  private boolean useHBaseInterface = true;
  
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
    
    // Defaults the user can override if needed
    config.set("google.bigtable.auth.service.account.enable", "true");
    config.set("hbase.client.connection.impl", 
        "com.google.cloud.bigtable.hbase1_0.BigtableConnection");
    
    // make it easy on ourselves by copying all CLI properties into the config object.
    Iterator<Entry<Object, Object>> it = props.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Object, Object> entry = it.next();
      config.set((String)entry.getKey(), (String)entry.getValue());
    }
    
    useHBaseInterface = getProperties().getProperty(USE_HBASE_API, "true")
         .equals("true") ? true : false;
    clientSideBuffering = getProperties().getProperty(CLIENT_SIDE_BUFFERING, "false")
        .equals("true") ? true : false;
    
    System.out.println("Running Google Bigtable with " + 
         (useHBaseInterface ? "HBase" : "Native") + " API" +
         (clientSideBuffering ? " and client side buffering." : "."));
    synchronized (threadCount) {
      ++threadCount;
      if (useHBaseInterface) {
        try {
          if (connection == null) {
            connection = ConnectionFactory.createConnection(config);
          }
        } catch (IOException e) {
          throw new DBException(e);
        }
      } else {
        if (session == null) {
          try {
            options = BigtableOptionsFactory.fromConfiguration(config);
            session = new BigtableSession(options);
            // important to instantiate the first client here, otherwise the
            // other threads may receive an NPE from the options when they try
            // to read the cluster name.
            client = session.getDataClient();
          } catch (IOException e) {
            throw new DBException("Error loading options from config: ", e);
          }
        } else {
          client = session.getDataClient();
        }
        
        if (clientSideBuffering) {
          heapSizeManager = new HeapSizeManager(
              Long.parseLong(
                  getProperties().getProperty(ASYNC_MUTATOR_MAX_MEMORY, 
                      Long.toString(AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT))),
              Integer.parseInt(
                  getProperties().getProperty(ASYNC_MAX_INFLIGHT_RPCS, 
                      Integer.toString(AsyncExecutor.MAX_INFLIGHT_RPCS_DEFAULT))));
          asyncExecutor = new AsyncExecutor(client, heapSizeManager);
        }
      }
    }
    
    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }
    
    final String columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);
  }
  
  @Override
  public void cleanup() throws DBException {
    if (useHBaseInterface) {
      synchronized (threadCount) {
        --threadCount;
        if (threadCount <= 0) {
          try {
            connection.close();
          } catch (IOException e) {
            throw new DBException(e);
          }
        }
      }
    } else {
      if (asyncExecutor != null) {
        try {
          asyncExecutor.flush();
        } catch (IOException e) {
          throw new DBException(e);
        }
      }
      synchronized (threadCount) {
        --threadCount;
        if (threadCount <= 0) {
          try {
            session.close();
          } catch (IOException e) {
            throw new DBException(e);
          }
        }
      }
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (debug) {
      System.out.println("Doing read from HBase columnfamily " 
          + new String(columnFamilyBytes));
      System.out.println("Doing read for key: " + key);
    }
    
    if (useHBaseInterface) {
      // if this is a "new" table, init HTable object. Else, use existing one
      if (!lastTable.equals(table)) {
        currentTable = null;
        try {
          getHTable(table);
        } catch (IOException e) {
          System.err.println("Error accessing HBase table: " + e);
          return Status.ERROR;
        }
      }
      
      Result r = null;
      try {
        Get g = new Get(Bytes.toBytes(key));
        if (fields == null) {
          g.addFamily(columnFamilyBytes);
        } else {
          for (String field : fields) {
            g.addColumn(columnFamilyBytes, Bytes.toBytes(field));
          }
        }
        r = currentTable.get(g);
      } catch (IOException e) {
        if (debug) {
          System.err.println("Error doing get: " + e);
        }
        return Status.ERROR;
      }

      if (r.isEmpty()) {
        return Status.NOT_FOUND;
      }

      while (r.advance()) {
        final Cell c = r.current();
        result.put(Bytes.toString(CellUtil.cloneQualifier(c)),
            new ByteArrayByteIterator(CellUtil.cloneValue(c)));
        if (debug) {
          System.out.println(
              "Result for field: " + Bytes.toString(CellUtil.cloneQualifier(c))
                  + " is: " + Bytes.toString(CellUtil.cloneValue(c)));
        }
      }
      return Status.OK;
    } else {
      // Native version
      setTable(table);
      
      RowFilter filter = RowFilter.newBuilder()
          .setFamilyNameRegexFilterBytes(ByteStringer.wrap(columnFamilyBytes))
          .build();
      if (fields != null && fields.size() > 0) {
        Builder filterChain = RowFilter.Chain.newBuilder();
        filterChain.addFilters(filter);
        filterChain.addFilters(RowFilter.newBuilder()
            .setCellsPerColumnLimitFilter(1)
            .build());
        int count = 0;
        // usually "field#" so pre-alloc
        final StringBuilder regex = new StringBuilder(fields.size() * 6);
        for (final String field : fields) {
          if (count++ > 0) {
            regex.append("|");
          }
          regex.append(field);
        }
        filterChain.addFilters(RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(
                ByteStringer.wrap(regex.toString().getBytes()))).build();
        filter = RowFilter.newBuilder().setChain(filterChain.build()).build();
      }
      
      final ReadRowsRequest.Builder rrr = ReadRowsRequest.newBuilder()
          .setTableNameBytes(ByteStringer.wrap(lastTableBytes))
          .setFilter(filter)
          .setRowKey(ByteStringer.wrap(key.getBytes()));
      
      List<Row> rows;
      try {
          rows = client.readRowsAsync(rrr.build()).get();
        if (rows == null || rows.isEmpty()) {
          return Status.NOT_FOUND;
        }
        for (final Row row : rows) {
          for (final Family family : row.getFamiliesList()) {
            if (Arrays.equals(family.getNameBytes().toByteArray(), columnFamilyBytes)) {
              for (final Column column : family.getColumnsList()) {
                // we should only have a single cell per column
                result.put(column.getQualifier().toString(UTF8_CHARSET), 
                    new ByteArrayByteIterator(column.getCells(0).getValue().toByteArray()));
                if (debug) {
                  System.out.println(
                      "Result for field: " + column.getQualifier().toString(UTF8_CHARSET)
                          + " is: " + column.getCells(0).getValue().toString(UTF8_CHARSET));
                }
              }
            }
          }
        }
        
        return Status.OK;
      } catch (InterruptedException e) {
        System.err.println("Interrupted during get: " + e);
        Thread.currentThread().interrupt();
        return Status.ERROR;
      } catch (ExecutionException e) {
        System.err.println("Exception during get: " + e);
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (useHBaseInterface) {
      // if this is a "new" table, init HTable object. Else, use existing one
      if (!lastTable.equals(table)) {
        currentTable = null;
        try {
          getHTable(table);
        } catch (IOException e) {
          System.err.println("Error accessing HBase table: " + e);
          return Status.ERROR;
        }
      }
      
      Scan s = new Scan(Bytes.toBytes(startkey));
      // HBase has no record limit. Here, assume recordcount is small enough to
      // bring back in one call.
      // We get back recordcount records
      s.setCaching(recordcount);
      if (this.usePageFilter) {
        s.setFilter(new PageFilter(recordcount));
      }

      // add specified fields or else all fields
      if (fields == null) {
        s.addFamily(columnFamilyBytes);
      } else {
        for (String field : fields) {
          s.addColumn(columnFamilyBytes, Bytes.toBytes(field));
        }
      }

      // get results
      ResultScanner scanner = null;
      try {
        scanner = currentTable.getScanner(s);
        int numResults = 0;
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
          // get row key
          String key = Bytes.toString(rr.getRow());

          if (debug) {
            System.out.println("Got scan result for key: " + key);
          }

          HashMap<String, ByteIterator> rowResult =
              new HashMap<String, ByteIterator>();

          while (rr.advance()) {
            final Cell cell = rr.current();
            rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
                new ByteArrayByteIterator(CellUtil.cloneValue(cell)));
          }

          // add rowResult to result vector
          result.add(rowResult);
          numResults++;
          
          if (numResults >= recordcount) {// if hit recordcount, bail out
            break;
          }
        } // done with row
      } catch (IOException e) {
        if (debug) {
          System.out.println("Error in getting/parsing scan result: " + e);
        }
        return Status.ERROR;
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }

      return Status.OK;
    } else {
      // Native version
      setTable(table);
      
      RowFilter filter = RowFilter.newBuilder()
          .setFamilyNameRegexFilterBytes(ByteStringer.wrap(columnFamilyBytes))
          .build();
      if (fields != null && fields.size() > 0) {
        Builder filterChain = RowFilter.Chain.newBuilder();
        filterChain.addFilters(filter);
        filterChain.addFilters(RowFilter.newBuilder()
            .setCellsPerColumnLimitFilter(1)
            .build());
        int count = 0;
        // usually "field#" so pre-alloc
        final StringBuilder regex = new StringBuilder(fields.size() * 6);
        for (final String field : fields) {
          if (count++ > 0) {
            regex.append("|");
          }
          regex.append(field);
        }
        filterChain.addFilters(RowFilter.newBuilder()
            .setColumnQualifierRegexFilter(
                ByteStringer.wrap(regex.toString().getBytes()))).build();
        filter = RowFilter.newBuilder().setChain(filterChain.build()).build();
      }
      
      final RowRange range = RowRange.newBuilder()
          .setStartKey(ByteStringer.wrap(startkey.getBytes()))
          .build();
      
      final ReadRowsRequest.Builder rrr = ReadRowsRequest.newBuilder()
          .setTableNameBytes(ByteStringer.wrap(lastTableBytes))
          .setFilter(filter)
          .setRowRange(range);
      
      List<Row> rows;
      try {
        rows = client.readRowsAsync(rrr.build()).get();
        if (rows == null || rows.isEmpty()) {
          return Status.NOT_FOUND;
        }
        int numResults = 0;
        
        for (final Row row : rows) {
          final HashMap<String, ByteIterator> rowResult =
              new HashMap<String, ByteIterator>(fields != null ? fields.size() : 10);
          
          for (final Family family : row.getFamiliesList()) {
            if (Arrays.equals(family.getNameBytes().toByteArray(), columnFamilyBytes)) {
              for (final Column column : family.getColumnsList()) {
                // we should only have a single cell per column
                rowResult.put(column.getQualifier().toString(UTF8_CHARSET), 
                    new ByteArrayByteIterator(column.getCells(0).getValue().toByteArray()));
                if (debug) {
                  System.out.println(
                      "Result for field: " + column.getQualifier().toString(UTF8_CHARSET)
                          + " is: " + column.getCells(0).getValue().toString(UTF8_CHARSET));
                }
              }
            }
          }
          
          result.add(rowResult);
          
          numResults++;
          if (numResults >= recordcount) {// if hit recordcount, bail out
            break;
          }
        }
        return Status.OK;
      } catch (InterruptedException e) {
        System.err.println("Interrupted during scan: " + e);
        Thread.currentThread().interrupt();
        return Status.ERROR;
      } catch (ExecutionException e) {
        System.err.println("Exception during scan: " + e);
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    
    if (useHBaseInterface) {
      // if this is a "new" table, init HTable object. Else, use existing one
      if (!lastTable.equals(table)) {
        currentTable = null;
        try {
          getHTable(table);
        } catch (IOException e) {
          System.err.println("Error accessing HBase table: " + e);
          return Status.ERROR;
        }
      }
      
      Put p = new Put(Bytes.toBytes(key));
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
    } else {
      setTable(table);
      
      final MutateRowRequest.Builder rowMutation = MutateRowRequest.newBuilder();
      rowMutation.setRowKey(ByteString.copyFromUtf8(key));
      rowMutation.setTableNameBytes(ByteStringer.wrap(lastTableBytes));
      
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        final Mutation.Builder mutationBuilder = rowMutation.addMutationsBuilder();
        final SetCell.Builder setCellBuilder = mutationBuilder.getSetCellBuilder();
        
        setCellBuilder.setFamilyNameBytes(ByteStringer.wrap(columnFamilyBytes));
        setCellBuilder.setColumnQualifier(ByteStringer.wrap(entry.getKey().getBytes()));
        setCellBuilder.setValue(ByteStringer.wrap(entry.getValue().toArray()));
        setCellBuilder.setTimestampMicros(-1); // set time to "now"
      }
      
      try {
        if (clientSideBuffering) {
          asyncExecutor.mutateRowAsync(rowMutation.build());
        } else {
          client.mutateRow(rowMutation.build());
        }
        return Status.OK;
      } catch (ServiceException e) {
        System.err.println("Failed to insert key: " + key + " " + e.getMessage());
        return Status.ERROR;
      } catch (InterruptedException e) {
        System.err.println("Interrupted while inserting key: " + key + " " 
            + e.getMessage());
        Thread.currentThread().interrupt();
        return Status.ERROR; // never get here, but lets make the compiler happy
      }
    }
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }
    
    if (useHBaseInterface) {
      // if this is a "new" table, init HTable object. Else, use existing one
      if (!lastTable.equals(table)) {
        currentTable = null;
        try {
          getHTable(table);
        } catch (IOException e) {
          System.err.println("Error accessing HBase table: " + e);
          return Status.ERROR;
        }
      }

      final Delete d = new Delete(Bytes.toBytes(key));
      try {
        if (clientSideBuffering) {
          Preconditions.checkNotNull(bufferedMutator);
          bufferedMutator.mutate(d);
        } else {
          currentTable.delete(d);
        }
        return Status.OK;
      } catch (IOException e) {
        System.err.println("Error doing delete: " + e);
        return Status.ERROR;
      }
    } else {
      setTable(table);
      
      final MutateRowRequest.Builder rowMutation = MutateRowRequest.newBuilder()
          .setRowKey(ByteString.copyFromUtf8(key))
          .setTableNameBytes(ByteStringer.wrap(lastTableBytes));
      rowMutation.addMutationsBuilder().setDeleteFromRow(
          DeleteFromRow.getDefaultInstance());
      
      try {
        if (clientSideBuffering) {
          asyncExecutor.mutateRowAsync(rowMutation.build());
        } else {
          client.mutateRow(rowMutation.build());
        }
        return Status.OK;
      } catch (ServiceException e) {
        System.err.println("Failed to delete key: " + key + " " + e.getMessage());
        return Status.ERROR;
      } catch (InterruptedException e) {
        System.err.println("Interrupted while delete key: " + key + " " 
            + e.getMessage());
        Thread.currentThread().interrupt();
        return Status.ERROR; // never get here, but lets make the compiler happy
      }
    }
  }

  /**
   * Little helper to set the table byte array. If it's different than the last
   * table we reset the byte array. Otherwise we just use the existing array.
   * @param table The table we're operating against
   */
  private void setTable(final String table) {
    if (!lastTable.equals(table)) {
      lastTable = table;
      lastTableBytes = options
          .getClusterName()
          .toTableName(table)
          .toString()
          .getBytes();
    }
  }
  
  /**
   * Pulled from HBaseClient10. Gets an HTable object from the connection object.
   * Make sure to cache the table name from the last operation so the thread 
   * avoids the heavy work of creating new table objects.
   * @param table The name of the table to find
   * @throws IOException If the table doesn't exist or the connection is gone
   */
  public void getHTable(String table) throws IOException {
    final TableName tName = TableName.valueOf(table);
    this.currentTable = this.connection.getTable(tName);
    lastTable = table;
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
