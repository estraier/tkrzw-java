/*************************************************************************************************
 * Example for serious use cases of the hash database
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

public class Example2 {
  public static void main(String[] args) {
    DBM dbm = new DBM();
    try {
      // Prepares the database, giving tuning parameters.
      Status status = dbm.open(
          "casket.tkh", true, "truncate=True,num_buckets=100");
      // Checks the status explicitly.
      if (!status.isOK()) {
        throw new StatusException(status);
      }
    
      // Sets records.
      // Throws an exception on failure.
      dbm.set("first", "hop").orDie();
      dbm.set("second", "step").orDie();
      dbm.set("third", "jump").orDie();

      // Retrieves record values.
      String[] keys = {"first", "second", "third", "fourth"};
      for (String key : keys) {
        // Gives a status object to check.
        String value = dbm.get(key, status);
        if (status.isOK()) {
          System.out.println(value);
        } else {
          System.err.println(status);
          if (!status.equals(Status.NOT_FOUND_ERROR)) {
            throw new StatusException(status);
          }
        }
      }

      // Traverses records.
      Iterator iter = dbm.makeIterator();
      try {
        iter.first();
        while (true) {
          String[] record = iter.getString(status);
          if (!status.isOK()) {
            if (!status.equals(Status.NOT_FOUND_ERROR)) {
              throw new StatusException(status);
            }
            break;
          }
          System.out.println(record[0] + ": " + record[1]);
          iter.next();
        }
      } finally {
        // Release the resources.
        iter.destruct();
      }

      // Closes the database.
      dbm.close().orDie();
    } finally {
      // Release the resources.
      dbm.destruct();
    }
  }
}
