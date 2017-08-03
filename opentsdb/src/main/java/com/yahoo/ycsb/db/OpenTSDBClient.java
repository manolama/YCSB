package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.Vector;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.workloads.TimeSeriesWorkload;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

/**
 * OpenTSDB client.
 */
public class OpenTSDBClient extends com.yahoo.ycsb.DB {
  private static final Logger LOG = (Logger)LoggerFactory.getLogger(OpenTSDBClient.class);
  
  public enum ClientType {
    NATIVE("native"),
    TELNET("telnet"),
    HTTP("http");
    
    private final String name;
    
    ClientType(final String name) {
      this.name = name;
    }
    
    public String getName() {
      return name;
    }
    
    public static ClientType fromString(final String name) {
      for (final ClientType type : ClientType.values()) {
        if (type.name.equalsIgnoreCase(name)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unrecognized client type: " + name);
    }
  }
  
  public static final String CONFIG_PROPERTY = "opentsdb.config";
  public static final String CONFIG_PROPERTY_DEFAULT = "/etc/opentsdb/os_opentsdb.conf";
  
  public static final String CLIENT_TYPE_PROPERTY = "opentsdb.client_type";
  public static final String CLIENT_TYPE_PROPERTY_DEFAULT = "native";
  
  public static final String ASYNC_PROPERTY = "opentsdb.async";
  public static final String ASYNC_PROPERTY_DEFAULT = "false";
  
  public static final String TSDB_HOST_PROPERTY = "opentsdb.host";
  public static final String TSDB_HOST_PROPERTY_DEFAULT = "http://localhost:4242";
  
  private static final Object MUTEX = new Object();
  private static int COUNTER = 0;
  private static TSDB TSDB_CLIENT;
  
  private boolean async;
  private ClientType clientType;
  private CloseableHttpAsyncClient httpClient;
  private String host;
  
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
    
    host = getProperties().getProperty(TSDB_HOST_PROPERTY, TSDB_HOST_PROPERTY_DEFAULT);
    clientType = ClientType.fromString(getProperties().getProperty(
        CLIENT_TYPE_PROPERTY, 
        CLIENT_TYPE_PROPERTY_DEFAULT));
    
    switch (clientType) {
    case NATIVE:
      try {
        synchronized (MUTEX) {
          if (COUNTER == 0) {
            final Config config = new Config(getProperties().getProperty(CONFIG_PROPERTY, CONFIG_PROPERTY_DEFAULT));
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
      break;
      
    case TELNET:
      break;
    case HTTP:
      httpClient = HttpAsyncClients.custom()
        .setDefaultIOReactorConfig(IOReactorConfig.custom()
            .setIoThreadCount(1).build())
        .setMaxConnTotal(1)
        .setMaxConnPerRoute(1)
        .build();
      httpClient.start();
      break;
    default:
      throw new IllegalArgumentException("Unsupported client type: " + clientType);
    }
    
    async = Boolean.parseBoolean(getProperties().getProperty(ASYNC_PROPERTY, ASYNC_PROPERTY_DEFAULT));
    
    timestampInterval = Integer.parseInt(getProperties().getProperty(
        TimeSeriesWorkload.TIMESTAMP_INTERVAL_PROPERTY, 
        TimeSeriesWorkload.TIMESTAMP_INTERVAL_PROPERTY_DEFAULT));
    tagPairDelimiter = getProperties().getProperty(
        TimeSeriesWorkload.PAIR_DELIMITER_PROPERTY, 
        TimeSeriesWorkload.PAIR_DELIMITER_PROPERTY_DEFAULT);
    queryTimeSpanDelimiter = getProperties().getProperty(
        TimeSeriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY,
        TimeSeriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
    groupByKey = getProperties().getProperty(
        TimeSeriesWorkload.GROUPBY_KEY_PROPERTY,
        TimeSeriesWorkload.GROUPBY_KEY_PROPERTY_DEFAULT);
    downsampleKey = getProperties().getProperty(
        TimeSeriesWorkload.DOWNSAMPLING_KEY_PROPERTY,
        TimeSeriesWorkload.DOWNSAMPLING_KEY_PROPERTY_DEFAULT);
  }
  
  @Override
  public void cleanup() throws DBException {
    switch (clientType) {
    case NATIVE:
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
      break;
    case HTTP:
      try {
        httpClient.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      break;
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

    final HashMap<String, String> tags = new HashMap<String, String>(values.size());
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      if (entry.getKey().equals(TimeSeriesWorkload.VALUE_KEY)) {
        final NumericByteIterator it = (NumericByteIterator) entry.getValue();
        if (isFloat = it.isFloatingPoint()) {
          doubleVal = it.getDouble();
        } else {
          longVal = it.getLong();
        }
      } else if (entry.getKey().equals(TimeSeriesWorkload.TIMESTAMP_KEY)) {
        timestamp = Utils.bytesToLong(entry.getValue().toArray());
      } else {
        tags.put(entry.getKey(), new String(entry.getValue().toString()));
      }
    }
    
    switch (clientType) {
    case NATIVE:
      try {
        WritableDataPoints wdps = TSDB_CLIENT.newDataPoints();
        wdps.setBatchImport(false);
        wdps.setBufferingTime((short)0);
        wdps.setSeries(key, tags);
        final Deferred<Object> deferred;
        if (isFloat) {
          deferred = wdps.addPoint(timestamp, (float) doubleVal);  
        } else {
          deferred = wdps.addPoint(timestamp, longVal);
        }
        
        if (!async) {
          deferred.join(500);
        }
        
        return Status.OK;
      } catch (Exception e) {
        LOG.error("WTF?", e);
        e.printStackTrace();
        return Status.ERROR;
      }
    case HTTP:
      final IncomingDataPoint dp = isFloat ? 
          new IncomingDataPoint(key, timestamp, Double.toString(doubleVal), tags) :
          new IncomingDataPoint(key, timestamp, Long.toString(longVal), tags);
      
      final HttpPost request = new HttpPost(host + "/api/put");
      request.setHeader("Content-Type", "application/json");
      request.setEntity(new ByteArrayEntity(JSON.serializeToBytes(dp)));
      
      if (!async) {
        try {
          httpClient.execute(request, null).get();
          return Status.OK;
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (ExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return Status.ERROR;
      } else {
        httpClient.execute(request, null);
        return Status.OK;
      }
    default:
      return Status.NOT_IMPLEMENTED;
    }
  }

  @Override
  public Status read(final String table, 
                     final String keyname, 
                     final Set<String> fields,
                     final HashMap<String, ByteIterator> results) {
    
    long timestamp = 0;
    final TSQuery query = new TSQuery();
    final TSSubQuery subQuery = new TSSubQuery();
    subQuery.setMetric(keyname);
    subQuery.setAggregator("sum");
    
    final List<TagVFilter> filters = Lists.newArrayList();
    for (final String field : fields) {
      //System.out.println("FIELD: " + field);
      final String[] pair = field.split(tagPairDelimiter);
      if (pair[0].equals(TimeSeriesWorkload.TIMESTAMP_KEY)) {
        final String[] range = pair[1].split(queryTimeSpanDelimiter);
        if (range.length == 1) {
          timestamp = Long.parseLong(range[0]);
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
        // 3 parts key, function, interval
        subQuery.setDownsample(pair[2] + "s-" + pair[1]);
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
    subQuery.setFilters(filters);
    query.setQueries((ArrayList<TSSubQuery>) Lists.newArrayList(subQuery)); 
    query.validateAndSetQuery();

    switch (clientType) {
    case NATIVE:
      try {
        final DataPoints[] response = query.buildQueries(TSDB_CLIENT)[0].run();
        System.out.println("QUERY: " + JSON.serializeToString(query));
        if (response.length < 1) {
          System.out.println("--------------------------------------------------------------------------");
          
          System.out.println("MATCH TIMESTAMP : " + timestamp);
          System.out.println("NO RESPONSE FOUND.........");
          return Status.NOT_FOUND;
        }
        for (final Entry<String, String> pair : response[0].getTags().entrySet()) {
          //System.out.println("PAIR: " + pair.getKey() + "  " + pair.getValue());
          results.put(pair.getKey(), new StringByteIterator(pair.getValue()));
        }
       
        final SeekableView iterator = response[0].iterator();
        while (iterator.hasNext()) {
          final DataPoint dp = iterator.next();
          //System.out.println("************************* DATA: " + dp.timestamp() + " " + dp.toDouble()+ "  isInteger: " + dp.isInteger());
          if (dp.timestamp() / 1000 == timestamp) {
            //System.out.println("matched, writing the value! DATA: " + dp.timestamp() + " " + dp.toDouble() + "  isInteger: " + dp.isInteger());
            results.put(TimeSeriesWorkload.TIMESTAMP_KEY, new NumericByteIterator(timestamp));
            results.put(TimeSeriesWorkload.VALUE_KEY, new NumericByteIterator(dp.isInteger() ? dp.longValue() : dp.doubleValue()));
            return Status.OK;
          }
        }
        return Status.NOT_FOUND;
      } catch (Exception e) {
        return Status.ERROR;
      }
      
    case HTTP:
      final HttpPost request = new HttpPost(host + "/api/query"
          + (!async ? "?sync" : ""));
      request.setHeader("Content-Type", "application/json");
      request.setEntity(new ByteArrayEntity(JSON.serializeToBytes(query)));
      try {
        httpClient.execute(request, null).get();
        return Status.OK;
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return Status.ERROR;
    default:
      return Status.NOT_IMPLEMENTED;
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
