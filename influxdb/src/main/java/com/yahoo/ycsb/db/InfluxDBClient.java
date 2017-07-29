package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.workloads.TimeseriesWorkload;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class InfluxDBClient extends com.yahoo.ycsb.DB {

  public enum ClientType {
    NATIVE("native"),
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
  
  public static final String BASE_URL_PROPERTY = "influxdb.base_url";
  public static final String BASE_URL_PROPERTY_DEFAULT = "http://localhost:8086";
  
  public static final String USER_PROPERTY = "influxdb.user";
  public static final String USER_PROPERTY_DEFAULT = "root";
  
  public static final String PASS_PROPERTY = "influxdb.pass";
  public static final String PASS_PROPERTY_DEFAULT = "root";
  
  public static final String DB_PROPERTY = "influxdb.db";
  public static final String DB_PROPERTY_DEFAULT = "ycsb";
  
  public static final String CLIENT_TYPE_PROPERTY = "influxdb.client_type";
  public static final String CLIENT_TYPE_PROPERTY_DEFAULT = "native";
  
  private static final Object MUTEX = new Object();
  private static int COUNTER = 0;
  private static InfluxDB NATIVE_CLIENT; 
  
  private OkHttpClient httpClient;
  private String protocol;
  private String host;
  private int port = 8086;
  private MediaType mediaType;
  private String baseURL;
  private String user;
  private String pass;
  private String db;
  
  private ClientType clientType;
  private TimeUnit timeUnits;
  private int threadId = 0;
  private int timestampInterval;
  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  private String groupByKey;
  private String downsampleKey;
  
  @Override
  public void init() throws DBException {
    baseURL = getProperties().getProperty(BASE_URL_PROPERTY, BASE_URL_PROPERTY_DEFAULT);
    user = getProperties().getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
    pass = getProperties().getProperty(PASS_PROPERTY, PASS_PROPERTY_DEFAULT);
    db = getProperties().getProperty(DB_PROPERTY, DB_PROPERTY_DEFAULT);
    
    clientType = ClientType.fromString(getProperties().getProperty(
        CLIENT_TYPE_PROPERTY, 
        CLIENT_TYPE_PROPERTY_DEFAULT));
    
    try {
      timeUnits = TimeUnit.valueOf(getProperties().getProperty(
          TimeseriesWorkload.TIMESTAMP_UNITS_PROPERTY, 
          TimeseriesWorkload.TIMESTAMP_UNITS_PROPERTY_DEFAULT).toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown time unit type", e);
    }
    if (timeUnits == TimeUnit.NANOSECONDS || timeUnits == TimeUnit.MICROSECONDS) {
      throw new IllegalArgumentException("YCSB doesn't support " + timeUnits + 
          " at this time.");
    }
    
    switch (clientType) {
    case NATIVE:
      synchronized (MUTEX) {
        if (COUNTER == 0) {
          NATIVE_CLIENT = InfluxDBFactory.connect(baseURL);
          NATIVE_CLIENT.setDatabase(db);
        }
        threadId = COUNTER++;
      }
      break;
    case HTTP:
      mediaType = MediaType.parse("text/plain");
      httpClient = new OkHttpClient();
      
      if (!baseURL.startsWith("http")) {
        throw new IllegalArgumentException("Base URL must start with 'http' or 'https'");
      }
      String[] parts = baseURL.split("://");
      protocol = parts[0];
      host = parts[1];
      if (host.contains(":")) {
        parts = host.split(":");
        host = parts[0];
        port = Integer.parseInt(parts[1]);
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported client type: " + clientType);
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
    switch (clientType) {
    case NATIVE:
      synchronized (MUTEX) {
        int countDown = --COUNTER;
        if (countDown < 1) {
          NATIVE_CLIENT.close();
          NATIVE_CLIENT = null;
          System.out.println("Successfully shutdown InfluxDB client on thread: " + threadId);
        }
      }
      break;
    case HTTP:
      //httpClient.close(); // WTF? How do you close this sucker and flush?
      break;
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    
    switch (clientType) {
    case NATIVE:
      throw new UnsupportedOperationException("No go yet.");
    case HTTP:
      
      final StringBuilder query = new StringBuilder()
        .append("SELECT \"value\" FROM \"")
        .append(key)
        .append("\" WHERE");
      
      int wheres = 0;
      for (final String field : fields) {
        if (wheres++ > 0) {
          query.append(" AND");
        }
        final String[] pair = field.split(tagPairDelimiter);
        if (pair[0].equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
          final String[] range = pair[1].split(queryTimeSpanDelimiter);
          if (range.length == 1) {
            query.append(" time == '")
                 .append(timestampToNano(Long.parseLong(range[0])))
                 .append("'");
          } else {
            query.append(" time >= '")
                 .append(timestampToNano(Long.parseLong(range[0])))
                 .append("' AND time <= '")
                 .append(timestampToNano(Long.parseLong(range[1])))
                 .append("'");
          }
        } else if (pair[0].equals(groupByKey)) {
          // TODO
        } else if (pair[0].equals(downsampleKey)) {
          // TODO
        } else {
          if (pair.length == 2) {
            // literal match
            query.append("'")
                 .append(pair[0])
                 .append("' = '")
                 .append(pair[1])
                 .append("'");
          } else {
            // TODO 
          }
        }
      }
      
      final HttpUrl url = new HttpUrl.Builder()
        .scheme(protocol)
        .host(host)
        .port(port)
        .addPathSegment("query")
        .addQueryParameter("db", db)
        .addQueryParameter("q", query.toString())
        .build();
      
      final Request request = new Request.Builder()
          .url(url)
          .build();
      Response response;
      try {
        response = httpClient.newCall(request).execute();
        if (!response.isSuccessful()) {
          System.out.println(response.message());
          return Status.ERROR;
        }
        System.out.println(response.body().string());
        return Status.OK;
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return Status.ERROR;
      }
    }
    return Status.ERROR;
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
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      switch (clientType) {
      case NATIVE:
        final Point.Builder point = Point.measurement(key);
        for (final Entry<String, ByteIterator> entry : values.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            if (it.isFloatingPoint()) {
              point.addField("value", it.getDouble());
            } else {
              point.addField("value", it.getLong());
            }
          } else if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
            point.time(Utils.bytesToLong(entry.getValue().toArray()), timeUnits);
          } else {
            point.tag(entry.getKey(), new String(entry.getValue().toString()));
          }
        }
        NATIVE_CLIENT.write(point.build());
        break;
      case HTTP:
        long longVal = 0;
        double doubleVal = 0;
        long timestamp = 0;
        boolean isFloat = false;

        final HashMap<String, String> tags = new HashMap<String, String>(values.size());
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
        
        final StringBuilder buf = new StringBuilder()
          .append(key);
        for (final Entry<String, String> pair : tags.entrySet()) {
          buf.append(",")
             .append(pair.getKey())
             .append("=")
             .append(pair.getValue());
        }
        
        buf.append(" value=")
           .append(isFloat ? Double.toString(doubleVal) : Long.toString(longVal))
           .append(" ")
           .append(timestampToNano(timestamp));
        final RequestBody body = RequestBody.create(mediaType, buf.toString());
        final Request post = new Request.Builder()
            .url(baseURL + "/write?db=" + db)
            .post(body)
            .build();
        final Response response = httpClient.newCall(post).execute();
        if (!response.isSuccessful()) {
          System.out.println(response.message());
          return Status.ERROR;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    // TODO Auto-generated method stub
    return null;
  }
  
  public long timestampToNano(final long timestamp) {
    switch (timeUnits) {
    case NANOSECONDS:
      return timestamp;
    case MILLISECONDS:
      return timestamp * 1000;
    case SECONDS:
      return timestamp * 1000 * 1000;
    default:
      throw new IllegalArgumentException("Not supporting units: " + timeUnits);
    }
  }
}
