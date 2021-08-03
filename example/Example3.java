/*************************************************************************************************
 * Example for the asynchronous API
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

import tkrzw.*;

public class Example3 {
  public static void main(String[] args) {
    // Prepares the database.
    DBM dbm = new DBM();
    dbm.open("casket.tkh", true, "truncate=True,num_buckets=100");

    // Prepares the asynchronous adapter with 4 worker threads.
    AsyncDBM async= new AsyncDBM(dbm, 4);

    // Executes the Set method asynchronously.
    Future<Status> set_future = async.set("hello", "world");
    // Does something in the foreground.
    System.out.println("Setting a record");
    // Checks the result after awaiting the set operation.
    Status status = set_future.get();
    if (!status.isOK()) {
      System.out.println("ERROR: " + status.toString());
    }

    // Executes the get method asynchronously.
    Future<Status.And<String>> get_future = async.get("hello");
    // Does something in the foreground.
    System.out.println("Getting a record");
    // Checks the result after awaiting the get operation.
    Status.And<String> get_result = get_future.get();
    if (get_result.status.isOK()) {
      System.out.println("VALUE: " + get_result.value);
    }

    // Releases the asynchronous adapter.
    async.destruct();

    // Closes the database.
    dbm.close();
  }
}

// END OF FILE
