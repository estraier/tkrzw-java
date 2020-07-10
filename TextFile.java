/*************************************************************************************************
 * Text file interface
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

/**
 * Text file of line data.
 * @note DBM#ExportKeysAsLines outputs keys of the database into a text file.  Scanning the
 * text file is more efficient than scanning the whole database.
 */
public class TextFile {
  static {
    Utility.loadLibrary();
  }

  /**
   * Constructor.
   */
  public TextFile() {
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
   * Opens a text file.
   * @param path The path of the file.
   * @return The result status.
   */
  public native Status open(String path);

  /**
   * Closes the text file.
   * @return The result status.
   */
  public native Status close();

  /**
   * Searches the text file and get lines which match a pattern.
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
