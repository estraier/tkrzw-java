/*************************************************************************************************
 * Iterator interface
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

package tkrzw;

import java.nio.charset.StandardCharsets;

/**
 * Iterator for each record.
 * @note An iterator is made by the "makeIterator" method of DBM.  Every unused iterator object
 * should be destructed explicitly by the "destruct" method to free resources.
 */
public class Iterator {
  static {
    Utility.loadLibrary();
  }

  /**
   * Constructor.
   * @param dbm The database to scan.
   */
  Iterator(DBM dbm) {
    initialize(dbm);
  }

  /**
   * Initialize the object.
   * @param dbm The database to scan.
   */
  private native void initialize(DBM dbm);

  /**
   * Destructs the object and releases resources.
   */
  public native void destruct();

  /**
   * Initializes the iterator to indicate the first record.
   * @return The result status.
   * @note Even if there's no record, the operation doesn't fail.
   */
  public native Status first();

  /**
   * Initializes the iterator to indicate the last record.
   * @return The result status.
   * @note Even if there's no record, the operation doesn't fail.  This method is suppoerted
   * only by ordered databases.
   */
  public native Status last();

  /**
   * Initializes the iterator to indicate a specific record.
   * @param key The key of the record to look for.
   * @return The result status.
   * @note Ordered databases can support "lower bound" jump; If there's no record with the
   * same key, the iterator refers to the first record whose key is greater than the given key.
   * The operation fails with unordered databases if there's no record with the same key.
   */
  public native Status jump(byte[] key);

  /**
   * Initializes the iterator to indicate a specific record, with string data.
   * @param key The key of the record to look for.
   * @return The result status.
   * @note Ordered databases can support "lower bound" jump; If there's no record with the
   * same key, the iterator refers to the first record whose key is greater than the given key.
   * The operation fails with unordered databases if there's no record with the same key.
   */
  public Status jump(String key) {
    return jump(key.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Initializes the iterator to indicate the last record whose key is lower than a given key.
   * @param key The key to compare with.
   * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
   * @return The result status.
   * @note Even if there's no matching record, the operation doesn't fail.  This method is
   * suppoerted only by ordered databases.
   */
  public native Status jumpLower(byte[] key, boolean inclusive);

  /**
   * Initializes the iterator to indicate the last record whose key is lower, with string data.
   * @param key The key to compare with.
   * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
   * @return The result status.
   * @note Even if there's no matching record, the operation doesn't fail.  This method is
   * suppoerted only by ordered databases.
   */
  public Status jumpLower(String key, boolean inclusive) {
    return jumpLower(key.getBytes(StandardCharsets.UTF_8), inclusive);
  }

  /**
   * Initializes the iterator to indicate the first record whose key is upper than a given key.
   * @param key The key to compare with.
   * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
   * @return The result status.
   * @note Even if there's no matching record, the operation doesn't fail.  This method is
   * suppoerted only by ordered databases.
   */
  public native Status jumpUpper(byte[] key, boolean inclusive);

  /**
   * Initializes the iterator to indicate the first record whose key is upper, with string data.
   * @param key The key to compare with.
   * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
   * @return The result status.
   * @note Even if there's no matching record, the operation doesn't fail.  This method is
   * suppoerted only by ordered databases.
   */
  public Status jumpUpper(String key, boolean inclusive) {
    return jumpUpper(key.getBytes(StandardCharsets.UTF_8), inclusive);
  }

  /**
   * Moves the iterator to the next record.
   * @return The result status.
   * @note If the current record is missing, the operation fails.  Even if there's no next
   * record, the operation doesn't fail.
   */
  public native Status next();

  /**
   * Moves the iterator to the previous record.
   * @return The result status.
   * @note If the current record is missing, the operation fails.  Even if there's no previous
   * record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
   */
  public native Status previous();

  /**
   * Gets the key and the value of the current record of the iterator.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public native byte[][] get(Status status);

  /**
   * Gets the key and the value of the current record of the iterator, witout status assingment.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public byte[][] get() {
    return get(null);
  }

  /**
   * Gets the key and the value of the current record, as string data.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public String[] getString(Status status) {
    byte[][] record = get(status);
    if (record == null) {
      return null;
    }
    String[] str_record = new String[2];
    str_record[0] = new String(record[0], StandardCharsets.UTF_8);
    str_record[1] = new String(record[1], StandardCharsets.UTF_8);
    return str_record;
  }

  /**
   * Gets the key and the value of the current record, as string data, without status assignemnt.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public String[] getString() {
    return getString(null);
  }

  /**
   * Gets the key of the current record.
   * @return The key of the current record, or null on failure.
   */
  public native byte[] getKey();

  /**
   * Gets the key of the current record, as string data.
   * @return The key of the current record, or null on failure.
   */
  public String getKeyString() {
    byte[] key = getKey();
    if (key == null) {
      return null;
    }
    return new String(key, StandardCharsets.UTF_8);
  }

  /**
   * Gets the key of the current record.
   * @return The key of the current record, or null on failure.
   */
  public native byte[] getValue();

  /**
   * Gets the key of the current record, as string data.
   * @return The key of the current record, or null on failure.
   */
  public String getValueString() {
    byte[] value = getValue();
    if (value == null) {
      return null;
    }
    return new String(value, StandardCharsets.UTF_8);
  }

  /**
   * Sets the value of the current record.
   * @param value The value of the record.
   * @return The result status.
   */
  public native Status set(byte[] value);

  /**
   * Sets the value of the current record, with string data.
   * @param value The value of the record.
   * @return The result status.
   */
  public Status set(String value) {
    return set(value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Removes the current record.
   * @return The result status.
   * @note If possible, the iterator moves to the next record.
   */
  public native Status remove();

  /**
   * Gets the current record and moves the iterator to the next record.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public native byte[][] step(Status status);

  /**
   * Gets the current record and moves the iterator to the next record, witout status assingment.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public byte[][] step() {
    return step(null);
  }

  /**
   * Gets the current record and moves the iterator to the next record, as string data.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public String[] stepString(Status status) {
    byte[][] record = step(status);
    if (record == null) {
      return null;
    }
    String[] str_record = new String[2];
    str_record[0] = new String(record[0], StandardCharsets.UTF_8);
    str_record[1] = new String(record[1], StandardCharsets.UTF_8);
    return str_record;
  }

  /**
   * Gets the current record and moves the iterator to the next record, as string data.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public String[] stepString() {
    return stepString(null);
  }

  /**
   * Jumps to the first record, removes it, and get the data.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public native byte[][] popFirst(Status status);

  /**
   * Jumps to the first record, removes it, and get the data, witout status assingment.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public byte[][] popFirst() {
    return popFirst(null);
  }

  /**
   * Jumps to the first record, removes it, and get the string data.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public String[] popFirstString(Status status) {
    byte[][] record = popFirst(status);
    if (record == null) {
      return null;
    }
    String[] str_record = new String[2];
    str_record[0] = new String(record[0], StandardCharsets.UTF_8);
    str_record[1] = new String(record[1], StandardCharsets.UTF_8);
    return str_record;
  }

  /**
   * Jumps to the first record, removes it, and get the string data, witout status assingment.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public String[] popFirstString() {
    return popFirstString(null);
  }

  /**
   * Gets a string representation of the iterator.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
