/*************************************************************************************************
 * Secondary index interface
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
import java.util.Map;

/**
 * Secondary index interface.
 * @note All operations except for open and close are thread-safe; Multiple threads can access
 * the same database concurrently.  You can specify a data structure when you call the "open"
 * method.  Every opened database must be closed explicitly by the "close" method to avoid data
 * corruption.  Moreover, every unused database object should be destructed by the "destruct"
 * method to free resources.
 */
public class Index {
  static {
    Utility.loadLibrary();
  }

  /**
   * Constructor.
   */
  public Index() {
    initialize();
  }

  /**
   * Initializes the object.
   */
  private native void initialize();

  /**
   * Destructs the object and releases resources.
   * @note The database is closed implicitly if it has not been closed.
   */
  public native void destruct();

  /**
   * Opens an index file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @return The result status.
   * @note If the path is empty, BabyDBM is used internally, which is equivalent to using the
   * MemIndex class.  If the path ends with ".tkt", TreeDBM is used internally, which is
   * equivalent to using the FileIndex class.  If the key comparator of the tuning parameter is
   * not set, PairLexicalKeyComparator is set implicitly.  Other compatible key comparators are
   * PairLexicalCaseKeyComparator, PairDecimalKeyComparator, PairHexadecimalKeyComparator, and
   * PairRealNumberKeyComparator.  Other options can be specified as with DBM::open.
   */
  public native Status open(String path, boolean writable, Map<String, String> params);

  /**
   * Opens an index file, with a string expression for optional parameters.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param params The optional parameter expression in "key=value,key=value" format.
   * @return The result status.
   */
  public Status open(String path, boolean writable, String params) {
    return open(path, writable, Utility.parseParams(params));
  }

  /**
   * Opens a database file, without optional parameters.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @return The result status.
   */
  public Status open(String path, boolean writable) {
    return open(path, writable, (Map<String, String>)null);
  }

  /**
   * Closes the database file.
   * @return The result status.
   */
  public native Status close();

  /**
   * Checks if a record exists or not.
   * @param key The key of the record.
   * @param value The key of the record.
   * @return True if the record exists, or false if not.
   */
  public native boolean contains(byte[] key, byte[] value);

  /**
   * Checks if a record exists or not, with string data.
   * @param key The key of the record.
   * @param value The key of the record.
   * @return True if the record exists, or false if not.
   */
  public boolean contains(String key, String value) {
    return contains(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Gets all values of records of a key.
   * @param key The key to look for.
   * @param max The maximum number of values to get.  0 means unlimited.
   * @return All values of the key.
   */
  public native byte[][] getValues(byte[] key, int max);

  /**
   * Gets all values of records of a key, with string data.
   * @param key The key to look for.
   * @param max The maximum number of values to get.  0 means unlimited.
   * @return All values of the key.
   */
  public String[] getValues(String key, int max) {
    byte[][] values = getValues(key.getBytes(StandardCharsets.UTF_8), max);
    if (values == null) {
      return null;
    }
    String[] strValues = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      strValues[i] = new String(values[i], StandardCharsets.UTF_8);
    }
    return strValues;
  }

  /**
   * Adds a record.
   * @param key The key of the record.  This can be an arbitrary expression to search the index.
   * @param value The value of the record.  This should be a primary value of another database.
   * @return The result status.
   */
  public native Status add(byte[] key, byte[] value);

  /**
   * Adds a record, with string data.
   * @param key The key of the record.  This can be an arbitrary expression to search the index.
   * @param value The value of the record.  This should be a primary value of another database.
   * @return The result status.
   */
  public Status add(String key, String value) {
    return add(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Removes a record.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The result status.
   */
  public native Status remove(byte[] key, byte[] value);

  /**
   * Removes a record, with string data.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The result status.
   */
  public Status remove(String key, String value) {
    return remove(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  public native long count();

  /**
   * Removes all records.
   * @return The result status.
   */
  public native Status clear();

  /**
   * Rebuilds the entire database.
   * @return The result status.
   */
  public native Status rebuild();

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The result status.
   */
  public native Status synchronize(boolean hard);

  /**
   * Checks whether the database is open.
   * @return True if the database is open, or false if not.
   */
  public native boolean isOpen();

  /**
   * Checks whether the database is writable.
   * @return True if the database is writable, or false if not.
   */
  public native boolean isWritable();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   * @note Every iterator should be destructed explicitly by the "destruct" method.
   */
  public native IndexIterator makeIterator();

  /**
   * Gets a string representation of the database.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
