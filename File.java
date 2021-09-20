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
 * @note All operations except for "open" and "close" are thread-safe; Multiple threads can
 * access the same file concurrently.  You can specify a concrete class when you call the
 * "open" method.  Every opened file must be closed explicitly by the "close" method to avoid
 * data corruption.  Moreover, every unused file object should be destructed by the "destruct"
 * method to free resources.
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
   * <li>sync_hard (bool): True to do physical synchronization when closing.
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
   * Opens a file, with a string expression for optional parameters.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param params The optional parameter expression in "key=value,key=value" format.
   * @return The result status.
   */
  public Status open(String path, boolean writable, String params) {
    return open(path, writable, Utility.parseParams(params));
  }

  /**
   * Opens a file, without optional parameters.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @return The result status.
   */
  public Status open(String path, boolean writable) {
    return open(path, writable, (Map<String, String>)null);
  }

  /**
   * Closes the file.
   * @return The result status.
   */
  public native Status close();

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param buf The destination buffer.
   * @param size The size to be read.
   * @return The result status.
   */
  public native Status read(long off, byte[] buf, long size);

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param size The size to be read.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A byte array containing read data.
   */
  public byte[] read(long off, long size, Status status) {
    byte[] buf = new byte[(int)size];
    Status tmp_status = read(off, buf, buf.length);
    if (status != null) {
      status.set(tmp_status.getCode(), tmp_status.getMessage());
    }
    return tmp_status.isOK() ? buf : null;
  }

  /**
   * Reads data and returns a byte array.
   * @param off The offset of a source region.
   * @param size The size to be read.
   * @return A byte array containing read data.
   */
  public byte[] read(long off, long size) {
    return read(off, size, null);
  }

  /**
   * Reads data and returns a byte array.
   * @param off The offset of a source region.
   * @param size The size to be read.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return A string containing read data.
   */
  public String readString(long off, long size, Status status) {
    byte[] buf = new byte[(int)size];
    Status tmp_status = read(off, buf, buf.length);
    if (status != null) {
      status.set(tmp_status.getCode(), tmp_status.getMessage());
    }
    return new String(buf, StandardCharsets.UTF_8);
  }

  /**
   * Reads data and returns a byte array.
   * @param off The offset of a source region.
   * @param size The size to be read.
   * @return A string containing read data.
   */
  public String readString(long off, long size) {
    return readString(off, size, null);
  }

  /**
   * Writes data.
   * @param off The offset of the destination region.
   * @param buf The source buffer.
   * @param size The size to be written.
   * @return The result status.
   */
  public native Status write(long off, byte[] buf, long size);

  /**
   * Writes data.
   * @param off The offset of the destination region.
   * @param buf The source buffer.  The written size is the size of the buffer.
   * @return The result status.
   */
  public Status write(long off, byte[] buf) {
    return write(off, buf, buf.length);
  }

  /**
   * Writes a string.
   * @param off The offset of the destination region.
   * @param str The source string.
   * @return The result status.
   */
  public Status write(long off, String str) {
    byte[] buf = str.getBytes(StandardCharsets.UTF_8);
    return write(off, buf, buf.length);
  }

  /**
   * Appends data at the end of the file.
   * @param buf The source buffer.
   * @param size The size to be written.
   * @param status The status object to store the result status.  If it is null, it is ignored.
   * @return The offset at which the data has been put, or -1 on failure.
   */
  public native long append(byte[] buf, long size, Status status);

  /**
   * Appends data at the end of the file.
   * @param buf The source buffer.  The written size is the size of the buffer.
   * @return The offset at which the data has been put, or -1 on failure.
   */
  public long append(byte[] buf) {
    return append(buf, buf.length, null);
  }

  /**
   * Appends data at the end of the file.
   * @param str The source string.
   * @return The offset at which the data has been put, or -1 on failure.
   */
  public long append(String str) {
    byte[] buf = str.getBytes(StandardCharsets.UTF_8);
    return append(buf);
  }

  /**
   * Truncates the file.
   * @param size The new size of the file.
   * @return The result status.
   * @note If the file is shrunk, data after the new file end is discarded.  If the file is
   * expanded, null codes are filled after the old file end.
   */
  public native Status truncate(long size);

  /**
   * Synchronizes the content of the file to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param off The offset of the region to be synchronized.
   * @param size The size of the region to be synchronized.  If it is zero, the length to the
   * end of file is specified.
   * @return The result status.
   * @note The pysical file size can be larger than the logical size in order to improve
   * performance by reducing frequency of allocation.  Thus, you should call this function before
   * accessing the file with external tools.
   */
  public native Status synchronize(boolean hard, long off, long size);

  /**
   * Synchronizes the entire content of the file to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The result status.
   */
  public Status synchronize(boolean hard) {
    return synchronize(hard, 0, 0);
  }

  /**
   * Gets the size of the file.
   * @return The size of the file or -1 on failure.
   */
  public native long getSize();

  /**
   * Gets the path of the file.
   * @return The path of the file or null on failure.
   */
  public native String getPath();

  /**
   * Searches the file and get lines which match a pattern.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return An array of lines matching the condition.
   */
  public native byte[][] search(String mode, byte[] pattern, int capacity);

  /**
   * Searches the file and get lines which match a pattern, with string data.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return An array of lines matching the condition.
   */
  public String[] search(String mode, String pattern, int capacity) {
    byte[][] result = search(mode, pattern.getBytes(StandardCharsets.UTF_8), capacity);
    String[] strResult = new String[result.length];
    for (int i = 0; i < result.length; i++) {
      strResult[i] = new String(result[i], StandardCharsets.UTF_8);
    }
    return strResult;
  }

  /**
   * Gets a string representation of the iterator.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
}

// END OF FILE
