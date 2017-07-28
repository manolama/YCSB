package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.workloads.TimeseriesWorkload;

public class BasicTSDB extends BasicDB {

  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  
  @Override
  public void init() {
    super.init();
    
    tagPairDelimiter = getProperties().getProperty(
        TimeseriesWorkload.PAIR_DELIMITER_PROPERTY, 
        TimeseriesWorkload.PAIR_DELIMITER_PROPERTY_DEFAULT);
    queryTimeSpanDelimiter = getProperties().getProperty(
        TimeseriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY,
        TimeseriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
  }
  
  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
            sb.append(entry.getKey()).append("=").append(Utils.bytesToLong(entry.getValue().toArray())).append(" ");
          } else if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            sb.append(entry.getKey()).append("=").append(it.isFloatingPoint() ? it.getDouble() : it.getLong()).append(" ");
          } else {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
          }
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    delay();

    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          if (entry.getKey().equals(TimeseriesWorkload.TIMESTAMP_KEY)) {
            sb.append(entry.getKey()).append("=").append(Utils.bytesToLong(entry.getValue().toArray())).append(" ");
          } else if (entry.getKey().equals(TimeseriesWorkload.VALUE_KEY)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            sb.append(entry.getKey()).append("=").append(it.isFloatingPoint() ? it.getDouble() : it.getLong()).append(" ");
          } else {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
          }
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    return Status.OK;
  }

}
