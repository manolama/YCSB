package com.yahoo.ycsb.generator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.yahoo.ycsb.Utils;

public class RandomPrintableStringGenerator extends Generator<String> {
  public final static int DEFAULTSTRINGLENGTH = 8;
  
  public final static Set<Integer> ALLBUTCONTROL;
  static {
    ALLBUTCONTROL = new HashSet<Integer>();
    // numbers
    ALLBUTCONTROL.add((int)Character.DECIMAL_DIGIT_NUMBER);
    ALLBUTCONTROL.add((int)Character.LETTER_NUMBER);
    ALLBUTCONTROL.add((int)Character.OTHER_NUMBER);
    
    // letters
    ALLBUTCONTROL.add((int)Character.UPPERCASE_LETTER);
    ALLBUTCONTROL.add((int)Character.LOWERCASE_LETTER);
    ALLBUTCONTROL.add((int)Character.TITLECASE_LETTER);
    ALLBUTCONTROL.add((int)Character.OTHER_LETTER);
    
    // marks
    ALLBUTCONTROL.add((int)Character.COMBINING_SPACING_MARK);
    ALLBUTCONTROL.add((int)Character.NON_SPACING_MARK);
    ALLBUTCONTROL.add((int)Character.ENCLOSING_MARK);
    
    // punctuation
    ALLBUTCONTROL.add((int)Character.CONNECTOR_PUNCTUATION);
    ALLBUTCONTROL.add((int)Character.DASH_PUNCTUATION);
    ALLBUTCONTROL.add((int)Character.START_PUNCTUATION);
    ALLBUTCONTROL.add((int)Character.END_PUNCTUATION);
    ALLBUTCONTROL.add((int)Character.INITIAL_QUOTE_PUNCTUATION);
    ALLBUTCONTROL.add((int)Character.FINAL_QUOTE_PUNCTUATION);
    ALLBUTCONTROL.add((int)Character.OTHER_PUNCTUATION);
    
    // symbols
    ALLBUTCONTROL.add((int)Character.MATH_SYMBOL);
    ALLBUTCONTROL.add((int)Character.CURRENCY_SYMBOL);
    ALLBUTCONTROL.add((int)Character.MODIFIER_SYMBOL);
    ALLBUTCONTROL.add((int)Character.OTHER_SYMBOL);
    
    // separators
    ALLBUTCONTROL.add((int)Character.SPACE_SEPARATOR);
    ALLBUTCONTROL.add((int)Character.LINE_SEPARATOR);
    ALLBUTCONTROL.add((int)Character.PARAGRAPH_SEPARATOR);
  }
  
  /**
   * Only decimals, upper and lower case letters.
   */
  public final static Set<Integer> BASICALPHATYPES;
  static {
    BASICALPHATYPES = new HashSet<Integer>(2);
    BASICALPHATYPES.add((int)Character.UPPERCASE_LETTER);
    BASICALPHATYPES.add((int)Character.LOWERCASE_LETTER);
  }
  
  /**
   * Only decimals, upper and lower case letters.
   */
  public final static Set<Integer> BASICALPHANUMERICTYPES;
  static {
    BASICALPHANUMERICTYPES = new HashSet<Integer>(3);
    BASICALPHANUMERICTYPES.add((int)Character.DECIMAL_DIGIT_NUMBER);
    BASICALPHANUMERICTYPES.add((int)Character.UPPERCASE_LETTER);
    BASICALPHANUMERICTYPES.add((int)Character.LOWERCASE_LETTER);
  }
  
  /**
   * Decimals, letter numbers, other numbers, upper, lower, title case as well
   * as letter modifiers and other letters.
   */
  public final static Set<Integer> EXTENDEDALPHANUMERICTYPES;
  static {
    EXTENDEDALPHANUMERICTYPES = new HashSet<Integer>(8);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.DECIMAL_DIGIT_NUMBER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.LETTER_NUMBER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.OTHER_NUMBER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.UPPERCASE_LETTER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.LOWERCASE_LETTER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.TITLECASE_LETTER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.MODIFIER_LETTER);
    EXTENDEDALPHANUMERICTYPES.add((int)Character.OTHER_LETTER);
  }
  
  private final int[] characterSet;
  
  private final int length;
  
  private String lastValue;
  
  public RandomPrintableStringGenerator() {
    this(DEFAULTSTRINGLENGTH);
  }
  
  public RandomPrintableStringGenerator(final int length) {
    this(length, getAlphaNumericASCIISet());
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
  
  public static int[] getFullASCIISet() {
    final List<Integer> validCharacters = 
        generateCharacterSet(0, 127, null, false, null);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }
  
  public static int[] getFullASCIISetWithNewlines() {
    final List<Integer> validCharacters =new ArrayList<Integer>();
    validCharacters.add(10); // newline
    validCharacters.addAll(generateCharacterSet(0, 127, null, false, null));
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }
  
  public static int[] getAlphaASCIISet() {
    final List<Integer> validCharacters = 
        generateCharacterSet(0, 127, null, false, BASICALPHATYPES);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }
  
  public static int[] getAlphaNumericASCIISet() {
    final List<Integer> validCharacters = 
        generateCharacterSet(0, 127, null, false, BASICALPHANUMERICTYPES);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  public static int[] getFullPlaneZeroSet() {
    final List<Integer> validCharacters = 
        generateCharacterSet(0, 65535, null, false, ALLBUTCONTROL);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }
  
  public static int[] getAlphaNumericPlaneZeroSet() {
    final List<Integer> validCharacters = 
        generateCharacterSet(0, 65535, null, false, BASICALPHANUMERICTYPES);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }
  
  public static List<Integer> generateCharacterSet(
      final int startCodePoint,
      final int lastCodePoint, 
      final Set<Integer> filter,
      final boolean isFilterAllowableList,
      final Set<Integer> allowableTypes) {
    
    // since we don't know the final size of the allowable character list we
    // start with a list then we'll flatten it to an array.
    final List<Integer> validCharacters = new ArrayList<Integer>(lastCodePoint);
    
    for (int codePoint = startCodePoint; codePoint <= lastCodePoint; codePoint++) {
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
      
      if (filter != null) {
        // if the filter is enabled then we need to make sure the code point 
        // is in the allowable list if it's a whitelist or that the code point
        // is NOT in the list if it's a blacklist.
        if ((isFilterAllowableList && !filter.contains(codePoint)) ||
            (filter.contains(codePoint))) {
          continue;
        }
      }
      
      validCharacters.add(codePoint);
    }
    return validCharacters;
  }

}
