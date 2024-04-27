/*************************************************************************************************
 * Index iterator interface
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
 * Iterator for each record of the secondary index.
 * @note An iterator is made by the "makeIterator" method of Index.  Every unused iterator object
 * should be destructed explicitly by the "destruct" method to free resources.
 */
public class IndexIterator {
  static {
    Utility.loadLibrary();
  }

  /**
   * Constructor.
   * @param index The index to scan.
   */
  IndexIterator(Index index) {
    initialize(index);
  }

  /**
   * Initialize the object.
   * @param index The index to scan.
   */
  private native void initialize(Index index);

  /**
   * Destructs the object and releases resources.
   */
  public native void destruct();

  /**
   * Initializes the iterator to indicate the first record.
   */
  public native void first();

  /**
   * Initializes the iterator to indicate the last record.
   */
  public native void last();

  /**
   * Initializes the iterator to indicate a specific range.
   * @param key The key of the lower bound.
   * @param value The value of the lower bound.
   */
  public native void jump(byte[] key, byte[] value);

  /**
   * Initializes the iterator to indicate a specific range, with string data.
   * @param key The key of the lower bound.
   * @param value The value of the lower bound.
   */
  public void jump(String key, String value) {
    jump(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Moves the iterator to the next record.
   */
  public native void next();

  /**
   * Moves the iterator to the previous record.
   */
  public native void previous();

  /**
   * Gets the key and the value of the current record of the iterator.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public native byte[][] get();

  /**
   * Gets the key and the value of the current record of the iterator.
   * @return A pair of the key and the value of the current record, or null on failure.
   */
  public String[] getString() {
    byte[][] record = get();
    if (record == null) {
      return null;
    }
    String[] str_record = new String[2];
    str_record[0] = new String(record[0], StandardCharsets.UTF_8);
    str_record[1] = new String(record[1], StandardCharsets.UTF_8);
    return str_record;
  }

  /**
   * Gets a string representation of the iterator.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
