/*************************************************************************************************
 * Example for secondary index
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

import java.util.HashMap;
import java.util.Map;
import tkrzw.*;

public class Example5 {
  public static void main(String[] args) {
    // Opens the index.
    Index index = new Index();
    index.open("casket.tkt", true, "truncate=True,num_buckets=100");

    // Adds records to the index.
    // The key is a division name and the value is person name.
    index.add("general", "anne").orDie();
    index.add("general", "matthew").orDie();
    index.add("general", "marilla").orDie();
    index.add("sales", "gilbert").orDie();

    // Anne moves to the sales division.
    index.remove("general", "anne").orDie();
    index.add("sales", "anne").orDie();

    // Prints all members for each division.
    String[] divisions = {"general", "sales"};
    for (String division : divisions) {
      System.out.println(division);
      String[] members = index.getValues(division, 0);
      for (String member : members) {
        System.out.println(" -- " + member);
      }
    }

    // Prints every record by iterator.
    IndexIterator iter = index.makeIterator();
    iter.first();
    while (true) {
      String[] record = iter.getString();
      if (record == null) break;
      System.out.println(record[0] + ": " + record[1]);
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    index.close().orDie();
    index.destruct();
  }
}

// END OF FILE
