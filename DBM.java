/*************************************************************************************************
 * Database manager interface
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
import java.util.HashMap;
import java.util.Map;

/**
 * Polymorphic database manager.
 * @note All operations except for open and close are thread-safe; Multiple threads can access
 * the same database concurrently.  You can specify a data structure when you call the "open"
 * method.  Every opened database must be closed explicitly by the "close" method to avoid data
 * corruption.  Moreover, every unused database object should be destructed by the "destruct"
 * method to free resources.
 */
public class DBM {
  static {
    Utility.loadLibrary();
  }

  /**
   * The special bytes value for no-operation or any data.
   */
  static public byte[] ANY_BYTES;

  /**
   * The special string value for no-operation or any data.
   */
  static public String ANY_STRING = new String("\0");

  /**
   * Constructor.
   */
  public DBM() {
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
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @return The result status.
   * @note The extension of the path indicates the type of the database.
   * <ul>
   * <li>.tkh : File hash database (HashDBM)
   * <li>.tkt : File tree database (TreeDBM)
   * <li>.tks : File skip database (SkipDBM)
   * <li>.tkmt : On-memory hash database (TinyDBM)
   * <li>.tkmb : On-memory tree database (BabyDBM)
   * <li>.tkmc : On-memory cache database (CacheDBM)
   * <li>.tksh : On-memory STL hash database (StdHashDBM)
   * <li>.tkst : On-memory STL tree database (StdTreeDBM)
   * </ul>
   * <p>The optional parameters can include options for the file opening operation.
   * <ul>
   * <li>truncate (bool): True to truncate the file.
   * <li>no_create (bool): True to omit file creation.
   * <li>no_wait (bool): True to fail if the file is locked by another process.
   * <li>no_lock (bool): True to omit file locking.
   * <li>sync_hard (bool): True to do physical synchronization when closing.
   * </ul>
   * <p>The optional parameter "dbm" supercedes the decision of the database type by the
   * extension.  The value is the type name: "HashDBM", "TreeDBM", "SkipDBM", "TinyDBM",
   * "BabyDBM", "CacheDBM", "StdHashDBM", "StdTreeDBM".
   * <p>The optional parameter "file" specifies the internal file implementation class.
   * The default file class is "MemoryMapAtomicFile".  The other supported classes are
   * "StdFile", "MemoryMapAtomicFile", "PositionalParallelFile", and "PositionalAtomicFile".
   * <p>For HashDBM, these optional parameters are supported.
   * <ul>
   * <li>update_mode (string): How to update the database file: "UPDATE_IN_PLACE" for the
   * in-palce or "UPDATE_APPENDING" for the appending mode.
   * <li>record_crc_mode (string): How to add the CRC data to the record: "RECORD_CRC_NONE"
   * to add no CRC to each record, "RECORD_CRC_8" to add CRC-8 to each record, "RECORD_CRC_16"
   * to add CRC-16 to each record, or "RECORD_CRC_32" to add CRC-32 to each record.
   * <li>record_comp_mode (string): How to compress the record data: "RECORD_COMP_NONE" to
   * do no compression, "RECORD_COMP_ZLIB" to compress with ZLib, "RECORD_COMP_ZSTD" to
   * compress with ZStd, "RECORD_COMP_LZ4" to compress with LZ4, "RECORD_COMP_LZMA" to
   * compress with LZMA.
   * <li>offset_width (int): The width to represent the offset of records.
   * <li>align_pow (int): The power to align records.
   * <li>num_buckets (int): The number of buckets for hashing.
   * <li>restore_mode (string): How to restore the database file: "RESTORE_SYNC" to restore to
   * the last synchronized state, "RESTORE_READ_ONLY" to make the database read-only, or
   * "RESTORE_NOOP" to do nothing.  By default, as many records as possible are restored.
   * <li>fbp_capacity (int): The capacity of the free block pool.
   * <li>min_read_size (int): The minimum reading size to read a record.
   * <li>cache_buckets (bool): True to cache the hash buckets on memory.
   * </ul>
   * <p>For TreeDBM, all optional parameters for HashDBM are available.  In addition, these
   * optional parameters are supported.
   * <ul>
   * <li>max_page_size (int): The maximum size of a page.
   * <li>max_branches (int): The maximum number of branches each inner node can have.
   * <li>max_cached_pages (int): The maximum number of cached pages.
   * <li>page_update_mode (string): What to do when each page is updated: "PAGE_UPDATE_NONE" is
   * to do no operation or "PAGE_UPDATE_WRITE" is to write immediately.
   * <li>key_comparator (string): The comparator of record keys: "LexicalKeyComparator" for
   * the lexical order, "LexicalCaseKeyComparator" for the lexical order ignoring case,
   * "DecimalKeyComparator" for the order of the decimal integer numeric expressions,
   * "HexadecimalKeyComparator" for the order of the hexadecimal integer numeric expressions,
   * "RealNumberKeyComparator" for the order of the decimal real number expressions.
   * </ul>
   * <p>For SkipDBM, these optional parameters are supported.
   * <ul>
   * <li>offset_width (int): The width to represent the offset of records.
   * <li>step_unit (int): The step unit of the skip list.
   * <li>max_level (int): The maximum level of the skip list.
   * <li>restore_mode (string): How to restore the database file: "RESTORE_SYNC" to restore to
   * the last synchronized state or "RESTORE_NOOP" to do nothing make the database read-only.
   * By default, as many records as possible are restored.
   * <li>sort_mem_size (int): The memory size used for sorting to build the database in the
   * at-random mode.
   * <li>insert_in_order (bool): If true, records are assumed to be inserted in ascending
   * order of the key.
   * <li>max_cached_records (int): The maximum number of cached records.
   * </ul>
   * <p>For TinyDBM, these optional parameters are supported.
   * <ul>
   * <li>num_buckets (int): The number of buckets for hashing.
   * </ul>
   * <p>For BabyDBM, these optional parameters are supported.
   * <ul>
   * <li>key_comparator (string): The comparator of record keys. The same ones as TreeDBM.
   * </ul>
   * <p>For CacheDBM, these optional parameters are supported.
   * <ul>
   * <li>cap_rec_num (int): The maximum number of records.
   * <li>cap_mem_size (int): The total memory size to use.
   * </ul>
   * <p>All databases support taking update logs into files.  It is enabled by setting the
   * prefix of update log files.
   * <ul>
   * <li>ulog_prefix (str): The prefix of the update log files.
   * <li>ulog_max_file_size (num): The maximum file size of each update log file.  By default,
   * it is 1GiB.
   * <li>ulog_server_id (num): The server ID attached to each log.  By default, it is 0.
   * <li>ulog_dbm_index (num): The DBM index attached to each log.  By default, it is 0.
   * </ul>
   * <p>For the file "PositionalParallelFile" and "PositionalAtomicFile", these optional
   * parameters are supported.
   * <ul>
   * <li>block_size (int): The block size to which all blocks should be aligned.
   * <li>access_options (str): Values separated by colon.  "direct" for direct I/O.  "sync" for
   * synchrnizing I/O, "padding" for file size alignment by padding, "pagecache" for the mini
   * page cache in the process.
   * </ul>
   * <p>If the optional parameter "num_shards" is set, the database is sharded into multiple
   * shard files.  Each file has a suffix like "-00003-of-00015".  If the value is 0, the number
   * of shards is set by patterns of the existing files, or 1 if they doesn't exist.
   */
  public native Status open(String path, boolean writable, Map<String, String> params);

  /**
   * Opens a database file, with a string expression for optional parameters.
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
   * @return True if the record exists, or false if not.
   */
  public native boolean contains(byte[] key);

  /**
   * Checks if a record exists or not, with string data.
   * @param key The key of the record.
   * @return True if the record exists, or false if not.
   */
  public boolean contains(String key) {
    return contains(key.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Gets the value of a record of a key.
   * @param key The key of the record.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return The value data of the record or null on failure.
   */
  public native byte[] get(byte[] key, Status status);

  /**
   * Gets the value of a record of a key, without status assignment.
   * @param key The key of the record.
   * @return The value data of the record or null on failure.
   */
  public byte[] get(byte[] key) {
    return get(key, null);
  }

  /**
   * Gets the value of a record of a key, with string data.
   * @param key The key of the record.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return The value data of the record or null on failure.
   */
  public native String get(String key, Status status);

  /**
   * Gets the value of a record of a key, with string data, without status assignment.
   * @param key The key of the record.
   * @return The value data of the record or null on failure.
   */
  public String get(String key) {
    return get(key, null);
  }

  /**
   * Gets the values of multiple records of keys.
   * @param keys The keys of records to retrieve.
   * @return A map of retrieved records.  Keys which don't match existing records are ignored.
   */
  public native Map<byte[], byte[]> getMulti(byte[][] keys);

  /**
   * Gets the values of multiple records of keys, with string data.
   * @param keys The keys of records to retrieve.
   * @return A map of retrieved records.  Keys which don't match existing records are ignored.
   */
  public native Map<String, String> getMulti(String[] keys);

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
   */
  public native Status set(byte[] key, byte[] value, boolean overwrite);

  /**
   * Sets a record of a key and a value, with overwriting.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The result status.
   */
  public Status set(byte[] key, byte[] value) {
    return set(key, value, true);
  }

  /**
   * Sets a record of a key and a value, with string data.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
   */
  public native Status set(String key, String value, boolean overwrite);

  /**
   * Sets a record of a key and a value, with string data, with overwriting.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The result status.
   */
  public Status set(String key, String value) {
    return set(key, value, true);
  }

  /**
   * Sets multiple records.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR
   * is returned.
   */
  public native Status setMulti(Map<byte[], byte[]> records, boolean overwrite);

  /**
   * Sets multiple records, with string data.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR
   * is returned.
   */
  public native Status setMultiString(Map<String, String> records, boolean overwrite);

  /**
   * Sets a record and get the old value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status and the old value.  If the record has not existed when inserting
   * the new record, null is assigned as the value.
   */
  public native Status.And<byte[]> setAndGet(byte[] key, byte[] value, boolean overwrite);

  /**
   * Sets a record and get the old value, with string data.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status and the old value.  If the record has not existed when inserting
   * the new record, null is assigned as the value.
   */
  public Status.And<String> setAndGet(String key, String value, boolean overwrite) {
    Status.And<byte[]> result = setAndGet(
        key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8),
        overwrite);
    Status.And<String> strResult = new Status.And<String>();
    strResult.status = result.status;
    if (result.value != null) {
      strResult.value = new String(result.value, StandardCharsets.UTF_8);
    }
    return strResult;
  }

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  public native Status remove(byte[] key);

  /**
   * Removes a record of a key, with string data.
   * @param key The key of the record.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  public native Status remove(String key);

  /**
   * Removes records of keys.
   * @param keys The keys of records to remove.
   * @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
   */
  public native Status removeMulti(byte[][] keys);

  /**
   * Removes records of keys, with string data.
   * @param keys The keys of records to remove.
   * @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
   */
  public native Status removeMulti(String[] keys);

  /**
   * Removes a record and get the value.
   * @param key The key of the record.
   * @return The result status and the record value.  If the record does not exist, null is
   * assigned
   */
  public native Status.And<byte[]> removeAndGet(byte[] key);

  /**
   * Removes a record and get the value.
   * @param key The key of the record.
   * @return The result status and the record value.  If the record does not exist, null is
   * assigned
   */
  public Status.And<String> removeAndGet(String key) {
    Status.And<byte[]> result = removeAndGet(key.getBytes(StandardCharsets.UTF_8));
    Status.And<String> strResult = new Status.And<String>();
    strResult.status = result.status;
    if (result.value != null) {
      strResult.value = new String(result.value, StandardCharsets.UTF_8);
    }
    return strResult;
  }

  /**
   * Appends data at the end of a record of a key.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Status append(byte[] key, byte[] value, byte[] delim);

  /**
   * Appends data at the end of a record of a key, with string data.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Status append(String key, String value, String delim);

  /**
   * Appends data to multiple records
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Status appendMulti(Map<byte[], byte[]> records, byte[] delim);

  /**
   * Appends data to multiple records, with string data.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @note If there's no existing record, the value is set without the delimiter.
   */
  public native Status appendMulti(Map<String, String> records, String delim);

  /**
   * Compares the value of a record and exchanges if the condition meets.
   * @param key The key of the record.
   * @param expected The expected value.  If it is null, no existing record is expected.  If it
   * is ANY_BYTES, an existing record with any value is expacted.
   * @param desired The desired value.  If it is null, the record is to be removed.  If it is
   * ANY_BYTES, no update is done.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  public native Status compareExchange(byte[] key, byte[] expected, byte[] desired);

  /**
   * Compares the value of a record and exchanges if the condition meets, with string data.
   * @param key The key of the record.
   * @param expected The expected value.  If it is null, no existing record is expected.  If it
   * is ANY_STRING, an existing record with any value is expacted.
   * @param desired The desired value.  If it is null, the record is to be removed.  If it is
   * ANY_STRING, no update is done.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  public Status compareExchange(String key, String expected, String desired) {
    byte[] rawExpected = null;
    if (expected == ANY_STRING) {
      rawExpected = ANY_BYTES;
    } else if (expected != null) {
      rawExpected = expected.getBytes(StandardCharsets.UTF_8);
    }
    byte[] rawDesired = null;
    if (desired == ANY_STRING) {
      rawDesired = ANY_BYTES;
    } else if (desired != null) {
      rawDesired = desired.getBytes(StandardCharsets.UTF_8);
    }
    return compareExchange(key.getBytes(StandardCharsets.UTF_8), rawExpected, rawDesired);
  }

  /**
   * Does compare-and-exchange and/or gets the old value of the record.
   * @param key The key of the record.
   * @param expected The expected value.  If it is null, no existing record is expected.  If it
   * is ANY_BYTES, an existing record with any value is expacted.
   * @param desired The desired value.  If it is null, the record is to be removed.  If it is
   * ANY_BYTES, no update is done.
   * @return The result status and the.old value of the record  If the condition doesn't meet,
   * the state is INFEASIBLE_ERROR.  If there's no existing record, the value is null.
   */
  public native Status.And<byte[]> compareExchangeAndGet(
      byte[] key, byte[] expected, byte[] desired);

  /**
   * Does compare-and-exchange and/or gets the old value of the record.
   * @param key The key of the record.
   * @param expected The expected value.  If it is null, no existing record is expected.  If it
   * is ANY_STRING, an existing record with any value is expacted.
   * @param desired The desired value.  If it is null, the record is to be removed.  If it is
   * ANY_STRING, no update is done.
   * @return The result status and the.old value of the record  If the condition doesn't meet,
   * the state is INFEASIBLE_ERROR.  If there's no existing record, the value is null.
   */
  public Status.And<String> compareExchangeAndGet(String key, String expected, String desired) {
    byte[] rawExpected = null;
    if (expected == ANY_STRING) {
      rawExpected = ANY_BYTES;
    } else if (expected != null) {
      rawExpected = expected.getBytes(StandardCharsets.UTF_8);
    }
    byte[] rawDesired = null;
    if (desired == ANY_STRING) {
      rawDesired = ANY_BYTES;
    } else if (desired != null) {
      rawDesired = desired.getBytes(StandardCharsets.UTF_8);
    }
    Status.And<byte[]> rawResult =
        compareExchangeAndGet(key.getBytes(StandardCharsets.UTF_8), rawExpected, rawDesired);
    Status.And<String> result = new Status.And<String>();
    result.status = rawResult.status;
    result.value = rawResult.value ==
        null ? null : new String(rawResult.value,  StandardCharsets.UTF_8);
    return result;
  }

  /**
   * Increments the numeric value of a record.
   * @param key The key of the record.
   * @param inc The incremental value.  If it is Long.MIN_VALUE, the current value is not changed
   * and a new record is not created.
   * @param init The initial value.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return The current value, or Long.MIN_VALUE on vailure
   * @note The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
   */
  public native long increment(byte[] key, long inc, long init, Status status);

  /**
   * Increments the numeric value of a record, with string data.
   * @param key The key of the record.
   * @param inc The incremental value.
   * @param init The initial value.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return The current value, or Long.MIN_VALUE on vailure
   * @note The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
   */
  public long increment(String key, long inc, long init, Status status) {
    return increment(key.getBytes(StandardCharsets.UTF_8), inc, init, status);
  }

  /**
   * Compares the values of records and exchanges if the condition meets.
   * @param expected The record keys and their expected values.  If the value is null, no existing
   * record is expected.  If the value is ANY_BYTES, an existing record with any value is
   * expacted.
   * @param desired The record keys and their desired values.  If the value is null, the record
   * is to be removed.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  public native Status compareExchangeMulti(
      Map<byte[], byte[]> expected, Map<byte[], byte[]> desired);

  /**
   * Compares the values of records and exchanges if the condition meets, with string data.
   * @param expected The record keys and their expected values.  If the value is null, no existing
   * record is expected.  If the value is ANY_STRING, an existing record with any value is
   * expacted.
   * @param desired The record keys and their desired values.  If the value is null, the record
   * is to be removed.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  public Status compareExchangeMultiString(
      Map<String, String> expected, Map<String, String> desired) {
    Map<byte[], byte[]> rawExpected = new HashMap<byte[], byte[]>();
    for (Map.Entry<String, String> record : expected.entrySet()) {
      byte[] rawKey = record.getKey().getBytes(StandardCharsets.UTF_8);
      String value = record.getValue();
      byte[] rawValue = null;
      if (value == ANY_STRING) {
        rawValue = ANY_BYTES;
      } else if (value != null) {
        rawValue = value.getBytes(StandardCharsets.UTF_8);
      }
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
   * @return The result status.  If there's no matching record to the old key, NOT_FOUND_ERROR
   * is returned.  If the overwrite flag is false and there is an existing record of the new key,
   * DUPLICATION ERROR is returned.
   * @note This method is done atomically.  The other threads observe that the record has either
   * the old key or the new key.  No intermediate states are observed.
   */
  public native Status rekey(
      byte[] oldKey, byte[] newKey, boolean overwrite, boolean copying);

  /**
   * Changes the key of a record, with string data.
   * @param oldKey The old key of the record.
   * @param newKey The new key of the record.
   * @param overwrite Whether to overwrite the existing record of the new key.
   * @param copying Whether to retain the record of the old key.
   * @return The result status.
   */
  public Status rekey(String oldKey, String newKey, boolean overwrite, boolean copying) {
    return rekey(oldKey.getBytes(StandardCharsets.UTF_8),
                 newKey.getBytes(StandardCharsets.UTF_8), overwrite, copying);
  }

  /**
   * Gets the first record and removes it.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public native byte[][] popFirst(Status status);

  /**
   * Gets the first record and removes it, without status assingment.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public byte[][] popFirst() {
    return popFirst(null);
  }

  /**
   * Gets the first record as strings and removes it.
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
   * Gets the first record as strings and removes it, without status assingment.
   * @return A pair of the key and the value of the first record, or null on failure.
   */
  public String[] popFirstString() {
    return popFirstString(null);
  }

  /**
   * Adds a record with a key of the current timestamp.
   * @param value The value of the record.
   * @param wtime The current wall time used to generate the key.  If it is negative, the system
   * clock is used.
   * @return The result status.
   * @note The key is generated as an 8-bite big-endian binary string of the timestamp.  If
   * there is an existing record matching the generated key, the key is regenerated and the
   * attempt is repeated until it succeeds.
   */
  public native Status pushLast(byte[] value, double wtime);

  /**
   * Adds a record with a key of the current timestamp.
   * @param value The value of the record.
   * @param wtime The current wall time used to generate the key.  If it is negative, the system
   * clock is used.
   * @return The result status.
   */
  public Status pushLast(String value, double wtime) {
    return pushLast(value.getBytes(StandardCharsets.UTF_8), wtime);
  }

  /**
   * Gets the number of records.
   * @return The number of records on success, or -1 on failure.
   */
  public native long count();

  /**
   * Gets the current file size of the database.
   * @return The current file size of the database, or -1 on failure.
   */
  public native long getFileSize();

  /**
   * Gets the path of the database file.
   * @return path The file path of the database, or null on failure.
   */
  public native String getFilePath();

  /**
   * Gets the timestamp in seconds of the last modified time.
   * @return The timestamp of the last modified time, or NaN on failure.
   */
  public native double getTimestamp();

  /**
   * Removes all records.
   * @return The result status.
   */
  public native Status clear();

  /**
   * Rebuilds the entire database.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @return The result status.
   * @note The optional parameters are the same as the Open method.  Omitted tuning parameters
   * are kept the same or implicitly optimized.  If it is null, it is ignored.
   * <p>In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
   * <ul>
   * <li>skip_broken_records (bool): If true, the operation continues even if there are broken
   * records which can be skipped.
   * <li>sync_hard (bool): If true, physical synchronization with the hardware is done before
   * finishing the rebuilt file.
   * </ul>
   */
  public native Status rebuild(Map<String, String> params);

  /**
   * Rebuilds the entire database, without optional parameters.
   * @return The result status.
   */
  public Status rebuild() {
    return rebuild(null);
  }

  /**
   * Checks whether the database should be rebuilt.
   * @return True to be optimized or false with no necessity.
   */
  public native boolean shouldBeRebuilt();

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @return The result status.
   * @note Only SkipDBM uses the optional parameters.  The "merge" parameter specifies paths
   * of databases to merge, separated by colon.  The "reducer" parameter specifies the reducer
   * to apply to records of the same key.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast",
   * etc are supported.
   */
  public native Status synchronize(boolean hard, Map<String, String> params);

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The result status.
   */
  public Status synchronize(boolean hard) {
    return synchronize(hard, null);
  }

  /**
   * Copies the content of the database file to another file.
   * @param destPath A path to the destination file.
   * @param syncHard True to do physical synchronization with the hardware.
   * @return The result status.
   */
  public native Status copyFileData(String destPath, boolean syncHard);

  /**
   * Exports all records to another database.
   * @param destDBM The destination database.
   * @return The result status.
   */
  public native Status export(DBM destDBM);

  /**
   * Exports all records of a database to a flat record file.
   * @param destFile The file object to write records in.
   * @return The result status.
   * @note A flat record file contains a sequence of binary records without any high level
   * structure so it is useful as a intermediate file for data migration.
   */
  public native Status exportToFlatRecords(File destFile);

  /**
   * Imports records to a database from a flat record file.
   * @param srcFile The file object to read records from.
   * @return The result status.
   */
  public native Status importFromFlatRecords(File srcFile);

  /**
   * Exports the keys of all records as lines to a text file.
   * @param destFile The file object to write keys in.
   * @return The result status.
   * @note As the exported text file is smaller than the database file, scanning the text file
   * by the search method is often faster than scanning the whole database.
   */
  public native Status exportKeysAsLines(File destFile);

  /**
   * Inspects the database.
   * @return A map of property names and their values.
   */
  public native Map<String, String> inspect();

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
   * Checks whether the database condition is healthy.
   * @return True if the database condition is healthy, or false if not.
   */
  public native boolean isHealthy();

  /**
   * Checks whether ordered operations are supported.
   * @return True if ordered operations are supported, or false if not.
   */
  public native boolean isOrdered();

  /**
   * Searches the database and get keys which match a pattern.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.  "containcase", "containword",
   * and "containcaseword" extract keys considering case and word boundary.  Ordered databases
   * support "upper" and "lower" which extract keys whose positions are upper/lower than the
   * pattern. "upperinc" and "lowerinc" are their inclusive versions.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return An array of keys matching the condition.
   */
  public native byte[][] search(String mode, byte[] pattern, int capacity);

  /**
   * Searches the database and get keys which match a pattern, with string data.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.  "containcase", "containword",
   * and "containcaseword" extract keys considering case and word boundary.  Ordered databases
   * support "upper" and "lower" which extract keys whose positions are upper/lower than the
   * pattern. "upperinc" and "lowerinc" are their inclusive versions.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return An array of keys matching the condition.
   */
  public String[] search(String mode, String pattern, int capacity) {
    byte[][] keys = search(mode, pattern.getBytes(StandardCharsets.UTF_8), capacity);
    String[] strKeys = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      strKeys[i] = new String(keys[i], StandardCharsets.UTF_8);
    }
    return strKeys;
  }

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   * @note Every iterator should be destructed explicitly by the "destruct" method.
   */
  public native Iterator makeIterator();

  /**
   * Gets a string representation of the database.
   */
  public native String toString();

  /**
   * Restores a broken database as a new healthy database.
   * @param oldFilePath The path of the broken database.
   * @param newFilePath The path of the new database to be created.
   * @param className The name of the database class.  If it is null or empty, the class is
   * guessed from the file extension.
   * @param endOffset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.  Using a positive value
   * is not meaningful if the number of shards is more than one.
   * @return The result status.
   */
  public static native Status restoreDatabase(
      String oldFilePath, String newFilePath, String className, long endOffset);

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
