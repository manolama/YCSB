/**                                                                                                                                                                                
 * Copyright (c) 2016 Yahoo! Inc. All rights reserved.                                                                                                                             
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
package com.yahoo.ycsb.generator;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class TestUnixEpochTimestampGenerator {

  @Test
  public void defaultCtor() throws Exception {
    final UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator();
    final long startTime = generator.currentValue();
    assertEquals(startTime + 60, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + 120, (long) generator.nextValue());
    assertEquals(startTime + 60, (long) generator.lastValue());
    assertEquals(startTime + 180, (long) generator.nextValue());
  }
  
  @Test
  public void ctorWithIntervalAndUnits() throws Exception {
    final UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator(120, TimeUnit.SECONDS);
    final long startTime = generator.currentValue();
    assertEquals(startTime + 120, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + 240, (long) generator.nextValue());
    assertEquals(startTime + 120, (long) generator.lastValue());
  }
  
  @Test
  public void ctorWithIntervalAndUnitsAndStart() throws Exception {
    final UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator(120, TimeUnit.SECONDS, 1072915200L);
    assertEquals(1072915200L, (long) generator.nextValue());
    assertEquals(1072915200L - 120, (long) generator.lastValue());
    assertEquals(1072915200L + 120, (long) generator.nextValue());
    assertEquals(1072915200L, (long) generator.lastValue());
  }
  
  @Test
  public void variousIntervalsAndUnits() throws Exception {
    // negatives could happen, just start and roll back in time
    UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator(-60, TimeUnit.SECONDS);
    long startTime = generator.currentValue();
    assertEquals(startTime - 60, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime - 120, (long) generator.nextValue());
    assertEquals(startTime - 60, (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.NANOSECONDS);
    startTime = generator.currentValue();
    assertEquals(startTime + 100, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + 200, (long) generator.nextValue());
    assertEquals(startTime + 100, (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.MICROSECONDS);
    startTime = generator.currentValue();
    assertEquals(startTime + 100, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + 200, (long) generator.nextValue());
    assertEquals(startTime + 100, (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.MILLISECONDS);
    startTime = generator.currentValue();
    assertEquals(startTime + 100, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + 200, (long) generator.nextValue());
    assertEquals(startTime + 100, (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.SECONDS);
    startTime = generator.currentValue();
    assertEquals(startTime + 100, (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + 200, (long) generator.nextValue());
    assertEquals(startTime + 100, (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(1, TimeUnit.MINUTES);
    startTime = generator.currentValue();
    assertEquals(startTime + (1 * 60), (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + (2 * 60), (long) generator.nextValue());
    assertEquals(startTime + (1 * 60), (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(1, TimeUnit.HOURS);
    startTime = generator.currentValue();
    assertEquals(startTime + (1 * 60 * 60), (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + (2 * 60 * 60), (long) generator.nextValue());
    assertEquals(startTime + (1 * 60 * 60), (long) generator.lastValue());
    
    generator = new UnixEpochTimestampGenerator(1, TimeUnit.DAYS);
    startTime = generator.currentValue();
    assertEquals(startTime + (1 * 60 * 60 * 24), (long) generator.nextValue());
    assertEquals(startTime, (long) generator.lastValue());
    assertEquals(startTime + (2 * 60 * 60 * 24), (long) generator.nextValue());
    assertEquals(startTime + (1 * 60 * 60 * 24), (long) generator.lastValue());
  }
  
  // TODO - With PowerMockito we could UT the initializeTimestamp(long) call.
  // Otherwise it would involve creating more functions and that would get ugly.
}
