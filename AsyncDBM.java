/*************************************************************************************************
 * Asynchronous database manager adapter
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
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Asynchronous database manager adapter.
 * @note This class is a wrapper of DBM for asynchronous operations.  A task queue with a thread
 * pool is used inside.  Every method except for the constructor and the destructor is run by a
 * thread in the thread pool and the result is set in the future oject of the return value.  The
 * caller can ignore the future object if it is not necessary.  The destruct method waits for all
 * tasks to be done.  Therefore, the destructor should be called before the database is closed.
 */
public class AsyncDBM {
  static {
    Utility.loadLibrary();
  }

  /**
   * Constructor.
   * @param dbm A database object which has been opened.
   * @param numWorkerThreads The number of threads in the internal thread pool.
   */
  public AsyncDBM(DBM dbm, int numWorkerThreads) {
    initialize(dbm, numWorkerThreads);
  }

  /**
   * Initializes the object.
   * @param dbm A database object which has been opened.
   * @param num_worker_threads The number of threads in the internal thread pool.
   */
  private native void initialize(DBM dbm, int numWorkerThreads);

  /**
   * Destructs the object and releases resources.
   * @note This method waits for all tasks to be done.
   */
  public native void destruct();

  /**
   * Gets the value of a record of a key.
   * @param key The key of the record.
   * @return The future for the status and the value data of the record.  If there's no matching
   * record, NOT_FOUND_ERROR is set.
   */
  public native Future<Status.And<byte[]>> get(byte[] key);

  /**
   * Gets the value of a record of a key, with string data.
   * @param key The key of the record.
   * @return The future for the status and the value data of the record.  If there's no matching
   * record, NOT_FOUND_ERROR is set.
   */
  public native Future<Status.And<String>> get(String key);

  /**
   * Gets the values of multiple records of keys.
   * @param keys The keys of records to retrieve.
   * @return A map of retrieved records.  Keys which don't match existing records are ignored.
   */
  public native Future<Status.And<Map<byte[], byte[]>>> getMulti(byte[][] keys);

  /**
   * Gets the values of multiple records of keys, with string data.
   * @param keys The keys of records to retrieve.
   * @return A map of retrieved records.  Keys which don't match existing records are ignored.
   */
  public native Future<Status.And<Map<String, String>>> getMulti(String[] keys);

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The future for the result status.  If overwriting is abandoned, DUPLICATION_ERROR is
   * set.
   */
  public native Future<Status> set(byte[] key, byte[] value, boolean overwrite);

  /**
   * Sets a record of a key and a value, with overwriting.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The future for the result status.
   */
  public Future<Status> set(byte[] key, byte[] value) {
    return set(key, value, true);
  }

  /**
   * Sets a record of a key and a value, with string data.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The future for the result status.  If overwriting is abandoned, DUPLICATION_ERROR is
   * set.
   */
  public native Future<Status> set(String key, String value, boolean overwrite);

  /**
   * Sets a record of a key and a value, with string data, with overwriting.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The future for the result status.
   */
  public Future<Status> set(String key, String value) {
    return set(key, value, true);
  }

  /**
   * Sets multiple records.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The future for the result status.  If there are records avoiding overwriting,
   * DUPLICATION_ERROR is set.
   */
  public native Future<Status> setMulti(Map<byte[], byte[]> records, boolean overwrite);

  /**
   * Sets multiple records, with string data.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The future for the result status.  If there are records avoiding overwriting,
   * DUPLICATION_ERROR is set.
   */
  public native Future<Status> setMultiString(Map<String, String> records, boolean overwrite);

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The future for the result status.  If there's no matching record, NOT_FOUND_ERROR is
   * set.
   */
  public native Future<Status> remove(byte[] key);

  /**
   * Removes a record of a key, with string data.
   * @param key The key of the record.
   * @return The future for the result status.  If there's no matching record, NOT_FOUND_ERROR is
   * set.
   */
  public native Future<Status> remove(String key);

  /**
   * Removes records of keys.
   * @param keys The keys of records to remove.
   * @return The future for the result status.  If there are missing records, NOT_FOUND_ERROR is
   * set.
   */
  public native Future<Status> removeMulti(byte[][] keys);

  /**
   * Removes records of keys, with string data.
   * @param keys The keys of records to remove.
   * @return The future for the result status.  If there are missing records, NOT_FOUND_ERROR is
   * set.
   */
  public native Future<Status> removeMulti(String[] keys);

  /**
   * Appends data at the end of a record of a key.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The future for the result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Future<Status> append(byte[] key, byte[] value, byte[] delim);

  /**
   * Appends data at the end of a record of a key, with string data.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The future for the result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Future<Status> append(String key, String value, String delim);

  /**
   * Appends data to multiple records
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The future for the result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Future<Status> appendMulti(Map<byte[], byte[]> records, byte[] delim);

  /**
   * Appends data to multiple records, with string data.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The future for the result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Future<Status> appendMulti(Map<String, String> records, String delim);

  /**
   * Compares the value of a record and exchanges if the condition meets.
   * @param key The key of the record.
   * @param expected The expected value.  If it is null, no existing record is expected.
   * @param desired The desired value.  If it is null, the record is to be removed.
   * @return The future for the result status.  If the condition doesn't meet, INFEASIBLE_ERROR
   * is set.
   */
  public native Future<Status> compareExchange(byte[] key, byte[] expected, byte[] desired);

  /**
   * Compares the value of a record and exchanges if the condition meets, with string data.
   * @param key The key of the record.
   * @param expected The expected value.  If it is null, no existing record is expected.
   * @param desired The desired value.  If it is null, the record is to be removed.
   * @return The future for the result status.  If the condition doesn't meet, INFEASIBLE_ERROR
   * is set.
   */
  public Future<Status> compareExchange(String key, String expected, String desired) {
    return compareExchange(key.getBytes(StandardCharsets.UTF_8),
                           expected == null ? null : expected.getBytes(StandardCharsets.UTF_8),
                           desired == null ? null : desired.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Increments the numeric value of a record.
   * @param key The key of the record.
   * @param inc The incremental value.  If it is Long.MIN_VALUE, the current value is not changed
   * and a new record is not created.
   * @param init The initial value.
   * @return The future for the result status and the current value.
   * @note The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
   */
  public native Future<Status.And<Long>> increment(byte[] key, long inc, long init);

  /**
   * Increments the numeric value of a record, with string data.
   * @param key The key of the record.
   * @param inc The incremental value.
   * @param init The initial value.
   * @return The future for the result status and the current value.
   * @note The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
   */
  public Future<Status.And<Long>> increment(String key, long inc, long init) {
    return increment(key.getBytes(StandardCharsets.UTF_8), inc, init);
  }

  /**
   * Compares the values of records and exchanges if the condition meets.
   * @param expected The record keys and their expected values.  If the value is null, no existing
   * record is expected.
   * @param desired The record keys and their desired values.  If the value is null, the record
   * is to be removed.
   * @return The future for the result status.  If the condition doesn't meet, INFEASIBLE_ERROR
   * is set.
   */
  public native Future<Status> compareExchangeMulti(
      Map<byte[], byte[]> expected, Map<byte[], byte[]> desired);

  /**
   * Compares the values of records and exchanges if the condition meets, with string data.
   * @param expected The record keys and their expected values.  If the data is null, no existing
   * record is expected.
   * @param desired The record keys and their desired values.  If the data is null, the record
   * is to be removed.
   * @return The future for the result status.  If the condition doesn't meet, INFEASIBLE_ERROR
   * is set.
   */
  public Future<Status> compareExchangeMultiString(
      Map<String, String> expected, Map<String, String> desired) {
    Map<byte[], byte[]> rawExpected = new HashMap<byte[], byte[]>();
    for (Map.Entry<String, String> record : expected.entrySet()) {
      byte[] rawKey = record.getKey().getBytes(StandardCharsets.UTF_8);
      String value = record.getValue();
      byte[] rawValue = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
      rawExpected.put(rawKey, rawValue);
    }
    Map<byte[], byte[]> rawDesired = new HashMap<byte[], byte[]>();
    for (Map.Entry<String, String> record : desired.entrySet()) {
      byte[] rawKey = record.getKey().getBytes(StandardCharsets.UTF_8);
      String value = record.getValue();
      byte[] rawValue = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
      rawDesired.put(rawKey, rawValue);
    }
    return compareExchangeMulti(rawExpected, rawDesired);
  }

  /**
   * Changes the key of a record.
   * @param oldKey The old key of the record.
   * @param newKey The new key of the record.
   * @param overwrite Whether to overwrite the existing record of the new key.
   * @param copying Whether to retain the record of the old key.
   * @return The future for the result status.  If there's no matching record to the old key,
   * NOT_FOUND_ERROR is set.  If the overwrite flag is false and there is an existing record of
   * the new key, DUPLICATION ERROR is set.
   * @note This method is done atomically.  The other threads observe that the record has either
   * the old key or the new key.  No intermediate states are observed.
   */
  public native Future<Status> rekey(
      byte[] oldKey, byte[] newKey, boolean overwrite, boolean copying);

  /**
   * Changes the key of a record, with string data.
   * @param oldKey The old key of the record.
   * @param newKey The new key of the record.
   * @param overwrite Whether to overwrite the existing record of the new key.
   * @param copying Whether to retain the record of the old key.
   * @return The future for the result status.
   */
  public Future<Status> rekey(String oldKey, String newKey, boolean overwrite, boolean copying) {
    return rekey(oldKey.getBytes(StandardCharsets.UTF_8),
                 newKey.getBytes(StandardCharsets.UTF_8), overwrite, copying);
  }

  /**
   * Gets the first record and removes it.
   * @return The future for the result status and a pair of the key and the value of the first
   * record.
   */
  public native Future<Status.And<byte[][]>> popFirst();

  /**
   * Gets the first record as strings and removes it.
   * @return The future for the result status and a pair of the key and the value of the first
   * record.
   */
  public native Future<Status.And<String[]>> popFirstString();

  /**
   * Adds a record with a key of the current timestamp.
   * @param value The value of the record.
   * @param wtime The current wall time used to generate the key.  If it is negative, the system
   * clock is used.
   * @return The future for the result status.
   * @note The key is generated as an 8-bite big-endian binary string of the timestamp.  If
   * there is an existing record matching the generated key, the key is regenerated and the
   * attempt is repeated until it succeeds.
   */
  public native Future<Status> pushLast(byte[] value, double wtime);

  /**
   * Adds a record with a key of the current timestamp.
   * @param value The value of the record.
   * @param wtime The current wall time used to generate the key.  If it is negative, the system
   * clock is used.
   * @return The future for the result status.
   * @note The key is generated as an 8-bite big-endian binary string of the timestamp.  If
   * there is an existing record matching the generated key, the key is regenerated and the
   * attempt is repeated until it succeeds.
   */
  public Future<Status> pushLast(String value, double wtime) {
    return pushLast(value.getBytes(StandardCharsets.UTF_8), wtime);
  }

  /**
   * Removes all records.
   * @return The future for the result status.
   */
  public native Future<Status> clear();

  /**
   * Rebuilds the entire database.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @return The future for the result status.
   * @note The parameters work in the same way as with DBM::rebuild.
   */
  public native Future<Status> rebuild(Map<String, String> params);

  /**
   * Rebuilds the entire database, without optional parameters.
   * @return The future for the result status.
   */
  public Future<Status> rebuild() {
    return rebuild(null);
  }

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @return The future for the result status.
   * @note The parameters work in the same way as with DBM::synchronize.
   */
  public native Future<Status> synchronize(boolean hard, Map<String, String> params);

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The future for the result status.
   */
  public Future<Status> synchronize(boolean hard) {
    return synchronize(hard, null);
  }

  /**
   * Copies the content of the database file to another file.
   * @param destPath A path to the destination file.
   * @param syncHard True to do physical synchronization with the hardware.
   * @return The future for the result status.
   */
  public native Future<Status> copyFileData(String destPath, boolean syncHard);

  /**
   * Exports all records to another database.
   * @param destDBM The destination database.  The lefetime of the database object must last
   * until the task finishes.
   * @return The future for the result status.
   */
  public native Future<Status> export(DBM destDBM);

  /**
   * Exports all records of a database to a flat record file.
   * @param destFile The file object to write records in.  The lefetime of the file object must
   * last until the task finishes.
   * @return The future for the result status.
   * @note A flat record file contains a sequence of binary records without any high level
   * structure so it is useful as a intermediate file for data migration.
   */
  public native Future<Status> exportToFlatRecords(File destFile);

  /**
   * Imports records to a database from a flat record file.
   * @param srcFile The file object to read records from.  The lefetime of the file object must
   * last until the task finishes.
   * @return The future for the result status.
   */
  public native Future<Status> importFromFlatRecords(File srcFile);

  /**
   * Searches the database and get keys which match a pattern.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return The future for the result status and an array of keys matching the condition.
   */
  public native Future<Status.And<byte[][]>> search(String mode, byte[] pattern, int capacity);

  /**
   * Searches the database and get keys which match a pattern, with string data.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return The future for the result status and an array of keys matching the condition.
   */
  public native Future<Status.And<String[]>> search(String mode, String pattern, int capacity);

  /**
   * Gets a string representation of the database.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
