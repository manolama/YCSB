/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.                                                                                                                             
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

package com.yahoo.ycsb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Utility functions.
 */
public class Utils
{
  private static final Random rand = new Random();
  private static final ThreadLocal<Random> rng = new ThreadLocal<Random>();

  public static Random random() {
    Random ret = rng.get();
    if(ret == null) {
      ret = new Random(rand.nextLong());
      rng.set(ret);
    }
    return ret;
  }
      /**
       * Generate a random ASCII string of a given length.
       */
      public static String ASCIIString(int length)
      {
	 int interval='~'-' '+1;
	
        byte []buf = new byte[length];
        random().nextBytes(buf);
        for (int i = 0; i < length; i++) {
          if (buf[i] < 0) {
            buf[i] = (byte)((-buf[i] % interval) + ' ');
          } else {
            buf[i] = (byte)((buf[i] % interval) + ' ');
          }
        }
        return new String(buf);
      }
      
      /**
       * Hash an integer value.
       */
      public static long hash(long val)
      {
	 return FNVhash64(val);
      }
	
      public static final int FNV_offset_basis_32=0x811c9dc5;
      public static final int FNV_prime_32=16777619;
      
      /**
       * 32 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
       * 
       * @param val The value to hash.
       * @return The hash value
       */
      public static int FNVhash32(int val)
      {
	 //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
	 int hashval = FNV_offset_basis_32;
	 
	 for (int i=0; i<4; i++)
	 {
	    int octet=val&0x00ff;
	    val=val>>8;
	    
	    hashval = hashval ^ octet;
	    hashval = hashval * FNV_prime_32;
	    //hashval = hashval ^ octet;
	 }
	 return Math.abs(hashval);
      }
      
      public static final long FNV_offset_basis_64=0xCBF29CE484222325L;
      public static final long FNV_prime_64=1099511628211L;
      
      /**
       * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
       * 
       * @param val The value to hash.
       * @return The hash value
       */
      public static long FNVhash64(long val)
      {
	 //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
	 long hashval = FNV_offset_basis_64;
	 
	 for (int i=0; i<8; i++)
	 {
	    long octet=val&0x00ff;
	    val=val>>8;
	    
	    hashval = hashval ^ octet;
	    hashval = hashval * FNV_prime_64;
	    //hashval = hashval ^ octet;
	 }
	 return Math.abs(hashval);
      }

      /**
       * Reads a big-endian 8-byte long from an offset in the given array.
       * @param bytes The array to read from.
       * @return A long integer.
       * @throws IndexOutOfBoundsException if the byte array is too small.
       * @throws NullPointerException if the byte array is null.
       */
      public static long bytesToLong(final byte[] bytes) {
        return (bytes[0] & 0xFFL) << 56
             | (bytes[1] & 0xFFL) << 48
             | (bytes[2] & 0xFFL) << 40
             | (bytes[3] & 0xFFL) << 32
             | (bytes[4] & 0xFFL) << 24
             | (bytes[5] & 0xFFL) << 16
             | (bytes[6] & 0xFFL) << 8
             | (bytes[7] & 0xFFL) << 0;
      }
      
      /**
       * Writes a big-endian 8-byte long at an offset in the given array.
       * @param val The value to encode.
       * @throws IndexOutOfBoundsException if the byte array is too small.
       */
      public static byte[] longToBytes(final long val) {
        final byte[] bytes = new byte[8];
        bytes[0] = (byte) (val >>> 56);
        bytes[1] = (byte) (val >>> 48);
        bytes[2] = (byte) (val >>> 40);
        bytes[3] = (byte) (val >>> 32);
        bytes[4] = (byte) (val >>> 24);
        bytes[5] = (byte) (val >>> 16);
        bytes[6] = (byte) (val >>>  8);
        bytes[7] = (byte) (val >>>  0);
        return bytes;
      }
      
      /**
       * Parses the byte array into a double.
       * The byte array must be at least 8 bytes long and have been encoded using 
       * {@link #doubleToBytes}. If the array is longer than 8 bytes, only the
       * first 8 bytes are parsed.
       * @param bytes The byte array to parse, at least 8 bytes.
       * @return A double value read from the byte array.
       * @throws IllegalArgumentException if the byte array is not 8 bytes wide.
       */
      public static double bytesToDouble(final byte[] bytes) {
        if (bytes.length < 8) {
          throw new IllegalArgumentException("Byte array must be 8 bytes wide.");
        }
        return Double.longBitsToDouble(bytesToLong(bytes));
      }
      
      /**
       * Encodes the double value as an 8 byte array.
       * @param val The double value to encode.
       * @return A byte array of length 8.
       */
      public static byte[] doubleToBytes(final double val) {
        return longToBytes(Double.doubleToRawLongBits(val));
      }
      
      /**
       * Set of all character types that include every symbol other than non-printable
       * control characters.
       */
      public final static Set<Integer> CHAR_TYPES_ALL_BUT_CONTROL;
      static {
        CHAR_TYPES_ALL_BUT_CONTROL = new HashSet<Integer>(24);
        // numbers
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.DECIMAL_DIGIT_NUMBER);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.LETTER_NUMBER);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.OTHER_NUMBER);
        
        // letters
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.UPPERCASE_LETTER);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.LOWERCASE_LETTER);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.TITLECASE_LETTER);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.OTHER_LETTER);
        
        // marks
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.COMBINING_SPACING_MARK);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.NON_SPACING_MARK);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.ENCLOSING_MARK);
        
        // punctuation
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.CONNECTOR_PUNCTUATION);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.DASH_PUNCTUATION);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.START_PUNCTUATION);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.END_PUNCTUATION);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.INITIAL_QUOTE_PUNCTUATION);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.FINAL_QUOTE_PUNCTUATION);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.OTHER_PUNCTUATION);
        
        // symbols
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.MATH_SYMBOL);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.CURRENCY_SYMBOL);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.MODIFIER_SYMBOL);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.OTHER_SYMBOL);
        
        // separators
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.SPACE_SEPARATOR);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.LINE_SEPARATOR);
        CHAR_TYPES_ALL_BUT_CONTROL.add((int)Character.PARAGRAPH_SEPARATOR);
      }
      
      /**
       * Set of character types including only decimals, upper and lower case letters.
       */
      public final static Set<Integer> CHAR_TYPES_BASIC_ALPHA;
      static {
        CHAR_TYPES_BASIC_ALPHA = new HashSet<Integer>(2);
        CHAR_TYPES_BASIC_ALPHA.add((int)Character.UPPERCASE_LETTER);
        CHAR_TYPES_BASIC_ALPHA.add((int)Character.LOWERCASE_LETTER);
      }
      
      /**
       * Set of character types including only  decimals, upper and lower case letters.
       */
      public final static Set<Integer> CHAR_TYPES_BASIC_ALPHANUMERICS;
      static {
        CHAR_TYPES_BASIC_ALPHANUMERICS = new HashSet<Integer>(3);
        CHAR_TYPES_BASIC_ALPHANUMERICS.add((int)Character.DECIMAL_DIGIT_NUMBER);
        CHAR_TYPES_BASIC_ALPHANUMERICS.add((int)Character.UPPERCASE_LETTER);
        CHAR_TYPES_BASIC_ALPHANUMERICS.add((int)Character.LOWERCASE_LETTER);
      }
      
      /**
       * Set of character types including only decimals, letter numbers, 
       * other numbers, upper, lower, title case as well as letter modifiers 
       * and other letters.
       */
      public final static Set<Integer> CHAR_TYPE_EXTENDED_ALPHANUMERICS;
      static {
        CHAR_TYPE_EXTENDED_ALPHANUMERICS = new HashSet<Integer>(8);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.DECIMAL_DIGIT_NUMBER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.LETTER_NUMBER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.OTHER_NUMBER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.UPPERCASE_LETTER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.LOWERCASE_LETTER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.TITLECASE_LETTER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.MODIFIER_LETTER);
        CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int)Character.OTHER_LETTER);
      }
      
      /**
       * Returns an array of printable code points with only the upper and lower
       * case alphabetical characters from the basic ASCII set.
       * @return An array of code points
       */
      public static int[] printableBasicAlphaASCIISet() {
        final List<Integer> validCharacters = 
            generatePrintableCharacterSet(0, 127, null, false, CHAR_TYPES_BASIC_ALPHA);
        final int[] characterSet = new int[validCharacters.size()];
        for (int i = 0; i < validCharacters.size(); i++) {
          characterSet[i] = validCharacters.get(i);
        }
        return characterSet;
      }
      
      /**
       * Returns an array of printable code points with the upper and lower case 
       * alphabetical characters as well as the numeric values from the basic 
       * ASCII set.
       * @return An array of code points
       */
      public static int[] printableBasicAlphaNumericASCIISet() {
        final List<Integer> validCharacters = 
            generatePrintableCharacterSet(0, 127, null, false, CHAR_TYPES_BASIC_ALPHANUMERICS);
        final int[] characterSet = new int[validCharacters.size()];
        for (int i = 0; i < validCharacters.size(); i++) {
          characterSet[i] = validCharacters.get(i);
        }
        return characterSet;
      }
      
      /**
       * Returns an array of printable code points with the entire basic ASCII table,
       * including spaces. Excludes new lines.
       * @return An array of code points
       */
      public static int[] fullPrintableBasicASCIISet() {
        final List<Integer> validCharacters = 
            generatePrintableCharacterSet(32, 127, null, false, null);
        final int[] characterSet = new int[validCharacters.size()];
        for (int i = 0; i < validCharacters.size(); i++) {
          characterSet[i] = validCharacters.get(i);
        }
        return characterSet;
      }
      
      /**
       * Returns an array of printable code points with the entire basic ASCII table,
       * including spaces and new lines.
       * @return An array of code points
       */
      public static int[] fullPrintableBasicASCIISetWithNewlines() {
        final List<Integer> validCharacters =new ArrayList<Integer>();
        validCharacters.add(10); // newline
        validCharacters.addAll(generatePrintableCharacterSet(32, 127, null, false, null));
        final int[] characterSet = new int[validCharacters.size()];
        for (int i = 0; i < validCharacters.size(); i++) {
          characterSet[i] = validCharacters.get(i);
        }
        return characterSet;
      }
      
      /**
       * Returns an array of printable code points the first plane of Unicode characters
       * including only the alpha-numeric values.
       * @return An array of code points
       */
      public static int[] printableAlphaNumericPlaneZeroSet() {
        final List<Integer> validCharacters = 
            generatePrintableCharacterSet(0, 65535, null, false, CHAR_TYPES_BASIC_ALPHANUMERICS);
        final int[] characterSet = new int[validCharacters.size()];
        for (int i = 0; i < validCharacters.size(); i++) {
          characterSet[i] = validCharacters.get(i);
        }
        return characterSet;
      }
      
      /**
       * Returns an array of printable code points the first plane of Unicode characters
       * including all printable characters.
       * @return An array of code points
       */
      public static int[] fullPrintablePlaneZeroSet() {
        final List<Integer> validCharacters = 
            generatePrintableCharacterSet(0, 65535, null, false, CHAR_TYPES_ALL_BUT_CONTROL);
        final int[] characterSet = new int[validCharacters.size()];
        for (int i = 0; i < validCharacters.size(); i++) {
          characterSet[i] = validCharacters.get(i);
        }
        return characterSet;
      }
      
      /**
       * Generates a list of code points based on a range and filters.
       * These can be used for generating strings with various ASCII and/or
       * Unicode printable character sets for use with DBs that may have 
       * character limitations.
       * <p>
       * Note that control, surrogate, format, private use and unassigned 
       * code points are skipped.
       * @param startCodePoint The starting code point, inclusive.
       * @param lastCodePoint The final code point, inclusive.
       * @param characterTypesFilter An optional set of allowable character
       * types. See {@link Character} for types.
       * @param isFilterAllowableList Determines whether the {@code allowableTypes}
       * set is inclusive or exclusive. When true, only those code points that
       * appear in the list will be included in the resulting set. Otherwise
       * matching code points are excluded.
       * @param allowableTypes An optional list of code points for inclusion or
       * exclusion.
       * @return A list of code points matching the given range and filters. The
       * list may be empty but is guaranteed not to be null.
       */
      public static List<Integer> generatePrintableCharacterSet(
          final int startCodePoint,
          final int lastCodePoint, 
          final Set<Integer> characterTypesFilter,
          final boolean isFilterAllowableList,
          final Set<Integer> allowableTypes) {
        
        // since we don't know the final size of the allowable character list we
        // start with a list then we'll flatten it to an array.
        final List<Integer> validCharacters = new ArrayList<Integer>(lastCodePoint);
        
        for (int codePoint = startCodePoint; codePoint <= lastCodePoint; ++codePoint) {
          if (allowableTypes != null && 
             !allowableTypes.contains(Character.getType(codePoint))) {
            continue;
          } else {
            // skip control points, formats, surrogates, etc
            final int type = Character.getType(codePoint);
            if (type == Character.CONTROL ||
                type == Character.SURROGATE ||
                type == Character.FORMAT ||
                type == Character.PRIVATE_USE ||
                type == Character.UNASSIGNED) {
              continue;
            }
          }
          
          if (characterTypesFilter != null) {
            // if the filter is enabled then we need to make sure the code point 
            // is in the allowable list if it's a whitelist or that the code point
            // is NOT in the list if it's a blacklist.
            if ((isFilterAllowableList && !characterTypesFilter.contains(codePoint)) ||
                (characterTypesFilter.contains(codePoint))) {
              continue;
            }
          }
          
          validCharacters.add(codePoint);
        }
        return validCharacters;
      }
}
