/*************************************************************************************************
 * Future interface
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
 * Future containing a status object and extra data.
 * @note Future objects are made by methods of AsyncDBM.  Every future object should be destroyed by the "destruct" method or the "get" method to free resources.
 */
public class Future<T> {
  /**
   * Default forbidden constructor.
   */
  private Future() {}

  /**
   * Destructs the object and releases resources.
   */
  public native void destruct();

  /**
   * Awaits the operation to be done.
   * @param timeout The waiting time in seconds.  If it is negative, no timeout is set.
   * @return True if the operation has done.  False if timeout occurs.
   */
  public native boolean await(double timeout);

  /**
   * Awaits the operation to be done and gets the result status.
   * @return The result status and extra data if any.  The existence and the type of extra data
   * depends on the operation which makes the future.  For DBM#Get, a tuple of the status and
   * the retrieved value is returned.  For DBM#Set and DBM#Remove, the status object itself is
   * returned.
   * @note The internal resource is released by this method.  "aait" and "get" cannot be called after calling this method.
   */
  public native T get();

  /**
   * Gets a string representation of the database.
   */
  public native String toString();

  /** The pointer to the native object */
  private long ptr_ = 0;
  /** Wheether the extra value is string data. */
  private boolean is_str_ = false;
}

// END OF FILE
