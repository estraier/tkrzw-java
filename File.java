/*************************************************************************************************
 * Generic file implementation
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
 * Generic file implementation.
 */
public class File {
  static {
    Utility.loadLibrary();
  }

  /**
   * Constructor.
   */
  public File() {
    initialize();
  }

  /**
   * Initializes the object.
   */
  private native void initialize();

  /**
   * Destructs the object and releases resources.
   * @note The file is closed implicitly if it has not been closed.
   */
  public native void destruct();

  /**
   * Opens a file.
   * @param path The path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param params Optional parameters.  If it is null, it is ignored.
   * @note The optional parameters can include options for the file opening operation.
   * <ul>
   * <li>truncate (bool): True to truncate the file.
   * <li>no_create (bool): True to omit file creation.
   * <li>no_wait (bool): True to fail if the file is locked by another process.
   * <li>no_lock (bool): True to omit file locking.
   * </ul>
   * <p>The optional parameter "file" specifies the internal file implementation class.
   * The default file class is "MemoryMapAtomicFile".  The other supported classes are
   * "StdFile", "MemoryMapAtomicFile", "PositionalParallelFile", and "PositionalAtomicFile".
   * <p>For the file "PositionalParallelFile" and "PositionalAtomicFile", these optional
   * parameters are supported.
   * <ul>
   * <li>block_size (int): The block size to which all blocks should be aligned.
   * <li>access_options (str): Values separated by colon.  "direct" for direct I/O.  "sync" for
   * synchrnizing I/O, "padding" for file size alignment by padding, "pagecache" for the mini
   * page cache in the process.
   * </ul>
   * @return The result status.
   */
  public native Status open(String path, boolean writable, Map<String, String> params);

  /**
   * Opens a file, without optional parameters.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @return The result status.
   */
  public Status open(String path, boolean writable) {
    return open(path, writable, null);
  }

  /**
   * Closes the file.
   * @return The result status.
   */
  public native Status close();

  /**
   * Searches the file and get lines which match a pattern.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the pattern is the least.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @param utf If true, text is treated as UTF-8, which affects "regex" and "edit".
   * @return An array of lines matching the condition.
   */
  public native String[] search(String mode, String pattern, int capacity, boolean utf);

  /**
   * Gets a string representation of the iterator.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
