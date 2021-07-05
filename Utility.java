/*************************************************************************************************
 * Utility interface
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

import java.util.HashMap;
import java.util.Map;

/**
 * Library utilities.
 */
public class Utility {
  static {
    loadLibrary();
  }

  /** The package version numbers. */
  static final String VERSION = getVersion();

  /**
   * Load the native library.
   */
  static synchronized void loadLibrary() {
    if(loaded) return;
    System.loadLibrary("jtkrzw");
    loaded = true;
  }

  /**
   * Gets the package version numbers.
   */
  private static native String getVersion();

  /**
   * Parses a string of parameters in "key=value,key=value" format.
   * @param str The string of parameters.
   * @return A map of key-value records.
   */
  public static HashMap<String, String> parseParams(String str) {
    HashMap<String, String> params = new HashMap<String, String>();
    String[] fields = str.split(",");
    for (String field : fields) {
      String[] columns = field.split("=", 2);
      if (columns.length == 2) {
        params.put(columns[0], columns[1]);
      }
    }
    return params;
  }

  /**
   * Gets the memory capacity of the platform.
   * @return The memory capacity of the platform in bytes, or -1 on failure.
   */
  public static native long getMemoryCapacity();

  /**
   * Gets the current memory usage of the process.
   * @return The current memory usage of the process in bytes, or -1 on failure.
   */
  public static native long getMemoryUsage();

  /**
   * Primary hash function for the hash database.
   * @param data The data to calculate the hash value for.
   * @param num_buckets The number of buckets of the hash table.  If it is 0 or less, the range
   * of the hash value is from Long.MIN_VALUE to Long.MAX_VALUE.
   * @return The hash value.
   */
  public static native long primaryHash(byte[] data, long num_buckets);

  /**
   * Secondaryy hash function for sharding.
   * @param data The data to calculate the hash value for.
   * @param num_shards The number of shards.  If it is 0 or less, the range of the hash value is
   * from Long.MIN_VALUE to Long.MAX_VALUE.
   * @return The hash value.
   */
  public static native long secondaryHash(byte[] data, long num_shards);

  /**
   * Gets the Levenshtein edit distance of two strings.
   * @param a A string.
   * @param b The other string.
   * @return The Levenshtein edit distance of the two strings.
   */
  public static native int editDistanceLev(String a, String b);

  /** The flag whether loaded. */
  static private boolean loaded = false;
}

// END OF FILE
