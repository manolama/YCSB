package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.Map.Entry;

import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.generator.UnixEpochTimestampGenerator;

import static org.junit.Assert.assertEquals;
import org.testng.annotations.Test;

public class TestTimeseriesWorkload {

  @Test
  public void foobar() throws Exception {
    final int records = 10;
    final Properties p = new Properties();
    p.put("recordcount", records);
    
    p.put("fieldcount", "1");
    p.put("tag_count", "2");
    p.put("tag_cardinality", "1,2");
    final TimeseriesWorkload wl = new TimeseriesWorkload();
    wl.init(p);
    
    final MockDB db = new MockDB();
    
    for (int i = 0; i < records + 64; i++) {
      wl.doInsert(db, null);
    }
    
    double val = 423423423212.352342346;
    byte[] b = Utils.doubleToBytes(val);
    assertEquals(val, Utils.bytesToDouble(b), 0.0001);
    
    long val2 = 32423425234563523L;
    b = Utils.longToBytes(val2);
    assertEquals(val2, Utils.bytesToLong(b));
  }
  
  @Test
  public void badTimeUnit() throws Exception {
    final Properties p = new Properties();
    p.put(UnixEpochTimestampGenerator.TIMESTAMP_UNITS_PROPERTY, "foobar");
    final TimeseriesWorkload wl = new TimeseriesWorkload();
    wl.init(p);
  }
  
  static class MockDB extends DB {

    @Override
    public Status read(String table, String key, Set<String> fields,
        HashMap<String, ByteIterator> result) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status update(String table, String key,
        HashMap<String, ByteIterator> values) {
      // TODO Auto-generated method stub
      return Status.OK;
    }

    @Override
    public Status insert(String table, String key,
        HashMap<String, ByteIterator> values) {
      System.out.print("Key: " + key + " Values: {");
      int i = 0;
      for (final Entry<String, ByteIterator> entry : values.entrySet()) {
        if (i > 0) {
          System.out.print(", ");
        }
        System.out.print("{" + entry.getKey() + " => ");
        if (entry.getKey().equals("YCSBV")) {
          System.out.print(new String(Utils.bytesToDouble(entry.getValue().toArray()) + "}"));  
        } else if (entry.getKey().equals("YCSBTS")) {
          System.out.print(new String(Utils.bytesToLong(entry.getValue().toArray()) + "}"));
        } else {
          System.out.print(new String(entry.getValue().toArray()) + "}");
        }
        
        ++i;
      }
      System.out.println("}");
      return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
      // TODO Auto-generated method stub
      return Status.OK;
    }
    
  }
}
