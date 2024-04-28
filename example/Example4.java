/*************************************************************************************************
 * Example for process methods
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

public class Example4 {
  public static void main(String[] args) {
    // Opens the database.
    DBM dbm = new DBM();
    dbm.open("casket.tkh", true, "truncate=True,num_buckets=100");

    // Sets records with lambda functions.
    dbm.process("doc-1", (k, v)->"Tokyo is the capital city of Japan.".getBytes(), true);
    dbm.process("doc-2", (k, v)->"Is she living in Tokyo, Japan?".getBytes(), true);
    dbm.process("doc-3", (k, v)->"She must leave Tokyo!".getBytes(), true);

    // Lowers record values.
    tkrzw.RecordProcessor lower = (key, value) -> {
      // If no matching record, None is given as the value.
      if (value == null) return null;
      // Sets the new value.
      return new String(value).toLowerCase().getBytes();
    };
    dbm.process("doc-1", lower, true);
    dbm.process("doc-2", lower, true);
    dbm.process("doc-3", lower, true);
    dbm.process("non-existent", lower, true);

    // Adds multiple records at once.
    RecordProcessor.WithKey[] ops1 = {
      new RecordProcessor.WithKey("doc-4", (k, v)->"Tokyo Go!".getBytes()),
      new RecordProcessor.WithKey("doc-5", (k, v)->"Japan Go!".getBytes()),
    };
    dbm.processMulti(ops1, true);

    // Modifies multiple records at once.
    RecordProcessor.WithKey[] ops2 = {
      new RecordProcessor.WithKey("doc-4", lower),
      new RecordProcessor.WithKey("doc-5", lower),
    };
    dbm.processMulti(ops2, true);

    // Checks the whole content.
    // This uses an external iterator and is relavively slow.
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

    // Opertion for word counting.
    Map<String, Integer> word_counts = new HashMap<String, Integer>();
    RecordProcessor wordCounter = (key, value) -> {
      if (key == null) return null;
      String[] words = new String(value).split("\\b");
      for (String word : words) {
        if (word.length() < 1) continue;
        char c = word.charAt(0);
        if (c < 'a' || c > 'z') continue;
        int old_count = word_counts.getOrDefault(word, 0);
        word_counts.put(word, old_count + 1);
      }
      return null;
    };

    // The second parameter should be false if the value is not updated.
    dbm.processEach(wordCounter, false);
    for(Map.Entry<String, Integer> entry : word_counts.entrySet()) {
      System.out.println(entry.getKey() + ":" + entry.getValue());
    }

    // Returning RecordProcessor.REMOVE by the callbacks removes the record.
    dbm.process("doc-1", (k, v)->RecordProcessor.REMOVE, true);
    System.out.println(dbm.count());
    RecordProcessor.WithKey[] ops3 = {
      new RecordProcessor.WithKey("doc-2", (k, v)->RecordProcessor.REMOVE),
      new RecordProcessor.WithKey("doc-3", (k, v)->RecordProcessor.REMOVE),
    };
    dbm.processMulti(ops3, true);
    System.out.println(dbm.count());
    dbm.processEach((k, v)->RecordProcessor.REMOVE, true);
    System.out.println(dbm.count());

    // Closes the database.
    dbm.close().orDie();
  }
}

// END OF FILE
