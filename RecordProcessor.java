/*************************************************************************************************
 * Record processor interface
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
 * Interface of processor for a record.
 */
public interface RecordProcessor {
  /**
   * The special string indicating removing operation.
   * @note The actual value is set by the native code.
   */
  public static final byte[] REMOVE = null;

  /**
   * Processes a record.
   * @param key The key of the existing record.
   * @param value The value of the existing record.
   * @return A string reference to NOOP, REMOVE, or a byte array of a new value.
   */
  public byte[] process(byte[] key, byte[] value);

  /**
   * Container of a processor and a key.
   */
  public static class WithKey {
    /**
     * Constructor.
     * @param key The key of the record to process.
     * @param processor The processor to process the record.
     */
    public WithKey(byte[] key, RecordProcessor processor) {
      this.processor = processor;
      this.key = key;
    }

    /**
     * Constructor.
     * @param key The key of the record to process.
     * @param processor The processor to process the record.
     */
    public WithKey(String key, RecordProcessor processor) {
      this.processor = processor;
      this.key = key.getBytes(StandardCharsets.UTF_8);
    }

    /** The key of the record to process. */
    public byte[] key;
    /** The processor object. */
    public RecordProcessor processor;
  }
}

// END OF FILE
