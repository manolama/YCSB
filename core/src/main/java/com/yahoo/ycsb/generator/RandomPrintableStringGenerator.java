/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

public class RandomPrintableStringGenerator extends Generator<String> {
  public final static int DEFAULTSTRINGLENGTH = 8;

  private final int[] characterSet;
  
  private final int length;
  
  private String lastValue;
  
  public RandomPrintableStringGenerator() {
    this(DEFAULTSTRINGLENGTH);
  }
  
  public RandomPrintableStringGenerator(final int length) {
    this(length, IncrementingPrintableStringGenerator.printableBasicAlphaNumericASCIISet());
  }
  
  public RandomPrintableStringGenerator(final int length, final int[] characterSet) {
    this.length = length;
    this.characterSet = characterSet;
  }
  
  @Override
  public String nextValue() {
    // TODO - instead of returning a String that's then converted to a byte array 
    // then to a string for time series utilities, just return a byte buffer that
    // can be cast ONCE at the last. Sheesh.
    final StringBuilder buffer = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      buffer.append(Character.toChars(
          characterSet[Utils.random().nextInt(characterSet.length - 1)]));
    }
    lastValue = buffer.toString();
    return lastValue;
  }
  
  @Override
  public String lastValue() {
    return lastValue;
  }
  
}