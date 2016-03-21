package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
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

import com.google.bigtable.repackaged.com.google.protobuf.BigtableZeroCopyByteStringUtil;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.repackaged.com.google.protobuf.ServiceException;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave.Builder;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Preconditions;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public class GoogleBigtableClient extends com.yahoo.ycsb.DB {
  private Configuration config = HBaseConfiguration.create();
  private Connection connection = null;
  private String tableName = "";
  
  //Depending on the value of clientSideBuffering, either bufferedMutator
  // (clientSideBuffering) or currentTable (!clientSideBuffering) will be used.
  private Table currentTable = null;
  private BufferedMutator bufferedMutator = null;
  
  private boolean debug = false;
    
  private static BigtableSession session;
  private BigtableDataClient client;
  private BigtableOptions options;
  
  private byte[] columnFamilyBytes = "cf".getBytes();
  
  private String lastTable = "";
  private byte[] lastTableBytes;
  
  private boolean useHTableInterface = true;
  
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
  private boolean clientSideBuffering = true;
  private long writeBufferSize = 1024 * 1024 * 12;
  
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    
    Iterator<Entry<Object, Object>> it = props.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Object, Object> entry = it.next();
      config.set((String)entry.getKey(), (String)entry.getValue());
    }
    
    if (useHTableInterface) {
      try {
        connection = ConnectionFactory.createConnection(config);
      } catch (IOException e) {
        throw new DBException(e);
      }
    } else {
      try {
        options = BigtableOptionsFactory.fromConfiguration(config);
        session = new BigtableSession(options);
      } catch (IOException e) {
        throw new DBException("Error loading options from config: ", e);
      }
      
      client = session.getDataClient();
    }
  }
  
  @Override
  public void cleanup() throws DBException {
    if (useHTableInterface) {
      try {
        connection.close();
      } catch (IOException e) {
        throw new DBException(e);
      }
    } else {
      try {
        session.close();
      } catch (IOException e) {
        throw new DBException(e);
      }
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (useHTableInterface) {
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
      
      Result r = null;
      try {
        if (debug) {
          System.out
              .println("Doing read from HBase columnfamily " + new String(columnFamilyBytes));
          System.out.println("Doing read for key: " + key);
        }
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
      } catch (ConcurrentModificationException e) {
        // do nothing for now...need to understand HBase concurrency model better
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
      // TODO - native
      RowFilter filter = RowFilter.newBuilder()
          .setFamilyNameRegexFilterBytes(ByteStringer.wrap(columnFamilyBytes))
          .build();
      if (fields != null && fields.size() > 0) {
        Builder filterChain = RowFilter.Interleave.newBuilder();
        filterChain.addFilters(filter);
        for (final String field : fields) {
          filterChain.addFilters(RowFilter.newBuilder()
              .setColumnQualifierRegexFilter(
                  ByteStringer.wrap(field.getBytes()))).build();
        }
        
        filter = RowFilter.newBuilder().setInterleave(filterChain.build()).build();
      }
      
      final ReadRowsRequest.Builder rrr = ReadRowsRequest.newBuilder()
          .setFilter(filter)
          .setRowKey(ByteString.copyFrom(key.getBytes()));
      
      com.google.cloud.bigtable.grpc.scanner.ResultScanner<Row> row = client.readRows(rrr.build());
      
      return Status.NOT_IMPLEMENTED;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (useHTableInterface) {
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

          // PageFilter does not guarantee that the number of results is <=
          // pageSize, so this
          // break is required.
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
      // TODO - native
      return Status.NOT_IMPLEMENTED;
    }
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    if (useHTableInterface) {
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
    } else {
      setTable(table);
      
      // TODO - async/buffered
      
      final MutateRowRequest.Builder rowMutation = MutateRowRequest.newBuilder();
      rowMutation.setRowKey(ByteString.copyFromUtf8(key));
      rowMutation.setTableNameBytes(BigtableZeroCopyByteStringUtil.wrap(lastTableBytes));
      
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        final Mutation.Builder mutationBuilder = rowMutation.addMutationsBuilder();
        final SetCell.Builder setCellBuilder = mutationBuilder.getSetCellBuilder();
        
        setCellBuilder.setFamilyNameBytes(BigtableZeroCopyByteStringUtil.wrap(columnFamilyBytes));
        setCellBuilder.setColumnQualifier(BigtableZeroCopyByteStringUtil.wrap(entry.getKey().getBytes()));
        setCellBuilder.setValue(BigtableZeroCopyByteStringUtil.wrap(entry.getValue().toArray()));
        setCellBuilder.setTimestampMicros(-1);
      }
      
      try {
        client.mutateRow(rowMutation.build());
        return Status.OK;
      } catch (ServiceException e) {
        System.err.println("Failed to insert key: " + key + " " + e.getMessage());
        return Status.ERROR;
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
    if (useHTableInterface) {
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
        System.out.println("Doing delete for key: " + key);
      }

      final Delete d = new Delete(Bytes.toBytes(key));
      d.setDurability(durability);
      try {
        if (clientSideBuffering) {
          Preconditions.checkNotNull(bufferedMutator);
          bufferedMutator.mutate(d);
        } else {
          currentTable.delete(d);
        }
        return Status.OK;
      } catch (IOException e) {
        if (debug) {
          System.err.println("Error doing delete: " + e);
        }
        return Status.ERROR;
      }
    } else {
      // TODO - native
      return Status.NOT_IMPLEMENTED;
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
      lastTableBytes = options.getClusterName().toTableName(table).toString().getBytes();
    }
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
