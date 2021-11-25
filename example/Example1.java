/*************************************************************************************************
 * Example for basic usage of the hash database
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

public class Example1 {
  public static void main(String[] args) {
    // Prepares the database.
    DBM dbm = new DBM();
    dbm.open("casket.tkh", true);
    
    // Sets records.
    // Keys and values are implicitly converted into byte arrays.
    dbm.set("first", "hop");
    dbm.set("second", "step");
    dbm.set("third", "jump");

    // Retrieves record values.
    // If the operation fails, null is returned.
    // If the class of the key is String, the value is converted into String.
    System.out.println(dbm.get("first"));
    System.out.println(dbm.get("second"));
    System.out.println(dbm.get("third"));
    System.out.println(dbm.get("fourth"));

    // Checks and deletes a record.
    if (dbm.contains("first")) {
      dbm.remove("first");
    }

    // Traverses records.
    // After using the iterator, it should be destructed explicitly.
    Iterator iter = dbm.makeIterator();
    iter.first();
    while (true) {
      String[] record = iter.getString();
      if (record == null) {
        break;
      }
      System.out.println(record[0] + ": " + record[1]);
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    // After using the database, it should be destructed explicitly.
    dbm.close();
    dbm.destruct();
  }
}

// END OF FILE
