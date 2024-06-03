/*************************************************************************************************
 * Example for key comparators of the tree database
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
    DBM dbm = new DBM();

    // Opens a new database with the default key comparator (LexicalKeyComparator).
    dbm.open("casket.tkt", true, "truncate=True").orDie();

    // Sets records with the key being a big-endian binary of an integer.
    // e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
    dbm.set(Utility.serializeInt(1), "hop".getBytes()).orDie();
    dbm.set(Utility.serializeInt(256), "step".getBytes()).orDie();
    dbm.set(Utility.serializeInt(32), "jump".getBytes()).orDie();

    // Gets records with the key being a big-endian binary of an integer.
    System.out.println(new String(dbm.get(Utility.serializeInt(1))));
    System.out.println(new String(dbm.get(Utility.serializeInt(256))));
    System.out.println(new String(dbm.get(Utility.serializeInt(32))));

    // Lists up all records, restoring keys into integers.
    Iterator iter = dbm.makeIterator();
    iter.first();
    while (true) {
      byte[][] record = iter.get();
      if (record == null) {
        break;
      }
      System.out.println(Utility.deserializeInt(record[0]) + ": " + new String(record[1]));
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    dbm.close().orDie();
    dbm.destruct();

    // Opens a new database with the decimal integer comparator.
    dbm.open("casket.tkt", true, "truncate=True,key_comparator=Decimal").orDie();

    // Sets records with the key being a decimal string of an integer.
    // e.g: "1" -> "hop"
    dbm.set("1", "hop").orDie();
    dbm.set("256", "step").orDie();
    dbm.set("32", "jump").orDie();

    // Gets records with the key being a decimal string of an integer.
    System.out.println(dbm.get("1"));
    System.out.println(dbm.get("256"));
    System.out.println(dbm.get("32"));

    // Lists up all records, restoring keys into integers.
    iter = dbm.makeIterator();
    iter.first();
    while (true) {
      String[] record = iter.getString();
      if (record == null) {
        break;
      }
      System.out.println(Long.parseLong(record[0]) + ": " + record[1]);
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    dbm.close().orDie();
    dbm.destruct();

    // Opens a new database with the decimal real number comparator.
    dbm.open("casket.tkt", true, "truncate=True,key_comparator=RealNumber").orDie();

    // Sets records with the key being a decimal string of a real number.
    // e.g: "1.5" -> "hop"
    dbm.set("1.5", "hop").orDie();
    dbm.set("256.5", "step").orDie();
    dbm.set("32.5", "jump").orDie();

    // Gets records with the key being a decimal string of a real number.
    System.out.println(dbm.get("1.5"));
    System.out.println(dbm.get("256.5"));
    System.out.println(dbm.get("32.5"));

    // Lists up all records, restoring keys into floating-point numbers.
    iter = dbm.makeIterator();
    iter.first();
    while (true) {
      String[] record = iter.getString();
      if (record == null) {
        break;
      }
      System.out.println(Double.parseDouble(record[0]) + ": " + record[1]);
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    dbm.close().orDie();
    dbm.destruct();

    // Opens a new database with the big-endian signed integers comparator.
    dbm.open("casket.tkt", true, "truncate=True,key_comparator=SignedBigEndian").orDie();

    // Sets records with the key being a big-endian binary of a floating-point number.
    // e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
    dbm.set(Utility.serializeInt(-1), "hop".getBytes()).orDie();
    dbm.set(Utility.serializeInt(-256), "step".getBytes()).orDie();
    dbm.set(Utility.serializeInt(-32), "jump".getBytes()).orDie();

    // Gets records with the key being a big-endian binary of a signed integer.
    System.out.println(new String(dbm.get(Utility.serializeInt(-1))));
    System.out.println(new String(dbm.get(Utility.serializeInt(-256))));
    System.out.println(new String(dbm.get(Utility.serializeInt(-32))));

    // Lists up all records, restoring keys into floating-point numbers.
    iter = dbm.makeIterator();
    iter.first();
    while (true) {
      byte[][] record = iter.get();
      if (record == null) {
        break;
      }
      System.out.println(Utility.deserializeInt(record[0]) + ": " + new String(record[1]));
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    dbm.close().orDie();
    dbm.destruct();

    // Opens a new database with the big-endian floating-point numbers comparator.
    dbm.open("casket.tkt", true, "truncate=True,key_comparator=FloatBigEndian").orDie();

    // Sets records with the key being a big-endian binary of a floating-point number.
    // e.g: "\x3F\xF8\x00\x00\x00\x00\x00\x00" -> "hop"
    dbm.set(Utility.serializeFloat(1.5), "hop".getBytes()).orDie();
    dbm.set(Utility.serializeFloat(256.5), "step".getBytes()).orDie();
    dbm.set(Utility.serializeFloat(32.5), "jump".getBytes()).orDie();

    // Gets records with the key being a big-endian binary of a floating-point number.
    System.out.println(new String(dbm.get(Utility.serializeFloat(1.5))));
    System.out.println(new String(dbm.get(Utility.serializeFloat(256.5))));
    System.out.println(new String(dbm.get(Utility.serializeFloat(32.5))));

    // Lists up all records, restoring keys into floating-point numbers.
    iter = dbm.makeIterator();
    iter.first();
    while (true) {
      byte[][] record = iter.get();
      if (record == null) {
        break;
      }
      System.out.println(Utility.deserializeFloat(record[0]) + ": " + new String(record[1]));
      iter.next();
    }
    iter.destruct();

    // Closes the database.
    dbm.close().orDie();
    dbm.destruct();
  }
}

// END OF FILE
