/*************************************************************************************************
 * Test cases
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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Test cases.
 */
public class Test {
  /**
   * Main routine of the test command.
   * @param args The command line arguments.
   */
  public static void main(String[] args) {
    if (args.length < 1) usage();
    int rv = 0;
    if (args[0].equals("utility")) {
      rv = runUtility();
    } else if (args[0].equals("status")) {
      rv = runStatus();
    } else if (args[0].equals("basic")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runBasic(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("iter")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runIter(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("thread")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runThread(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("search")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runSearch(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("export")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runExport(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("asyncdbm")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runAsyncDBM(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("file")) {
      String tmp_dir_path = createTempDir();
      try {
        rv = runFile(tmp_dir_path);
      } finally {
        removeDirectory(tmp_dir_path);
      }
    } else if (args[0].equals("perf")) {
      String path = "";
      int num_iterations = 10000;
      int num_threads = 1;
      String params = "";
      boolean is_random = false;
      for (int i = 1; i < args.length; i++) {
        String arg = args[i];
        if (arg.equals("--path")) {
          i++;
          path = args[i];
        } else if (arg.equals("--iter")) {
          i++;
          num_iterations = atoi(args[i]);
        } else if (arg.equals("--threads")) {
          i++;
          num_threads = atoi(args[i]);
        } else if (arg.equals("--params")) {
          i++;
          params = args[i];
        } else if (arg.equals("--random")) {
          is_random = true;
        } else {
          usage();
        }
      }
      rv = runPerf(path, num_iterations, num_threads, params, is_random);
    } else if (args[0].equals("wicked")) {
      String path = "";
      int num_iterations = 10000;
      int num_threads = 1;
      String params = "";
      for (int i = 1; i < args.length; i++) {
        String arg = args[i];
        if (arg.equals("--path")) {
          i++;
          path = args[i];
        } else if (arg.equals("--iter")) {
          i++;
          num_iterations = atoi(args[i]);
        } else if (arg.equals("--threads")) {
          i++;
          num_threads = atoi(args[i]);
        } else if (arg.equals("--params")) {
          i++;
          params = args[i];
        } else {
          usage();
        }
      }
      rv = runWicked(path, num_iterations, num_threads, params);
    } else {
      usage();
    }
    System.gc();
    System.exit(rv);
  }

  /**
   * Prints the usage and exit.
   */
  private static void usage() {
    STDERR.printf("test cases of the Java binding\n");
    STDERR.printf("\n");
    STDERR.printf("synopsis:\n");
    STDERR.printf("  java -ea %s command arguments...\n", Test.class.getName());
    STDERR.printf("\n");
    STDERR.printf("command and arguments:\n");
    STDERR.printf("  utility\n");
    STDERR.printf("  status\n");
    STDERR.printf("  basic\n");
    STDERR.printf("  thread\n");
    STDERR.printf("  iter\n");
    STDERR.printf("  search\n");
    STDERR.printf("  export\n");
    STDERR.printf("  asyncdbm\n");
    STDERR.printf("  file\n");
    STDERR.printf("  perf [--path num] [--iter num] [--threads num] [--params expr] [--random]\n");
    STDERR.printf("\n");
    System.exit(1);
  }

  /**
   * Creates a temporary directory which is to be removed on exit.
   */
  private static String createTempDir() {
    try {
      String tmp_dir_path = Files.createTempDirectory("tkrzw-java-").toString();
      return tmp_dir_path;
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Removes a temporary directory recursively.
   */
  private static void removeDirectory(String path) {
    try {
      Files.walk(Paths.get(path)).sorted(Comparator.reverseOrder())
          .map(Path::toFile).forEach(java.io.File::delete);
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts a string into an integer.
   */
  private static int atoi(String str) {
    try {
      return Integer.parseInt(str);
    } catch(NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Throws an error status if an expression is false.
   */
  private static void check(boolean expr) {
    if (!expr) {
      throw new AssertionError("check failed");
    }
  }

  /**
   * Gets the current timestamp.
   */
  private static double getTime() {
    Instant instant = Instant.now();
    return instant.getEpochSecond() + instant.getNano() / 1000000000.0;
  }

  /**
   * Makes a string map of the given key/value pairs.
   */
  private static Map<String, String> makeStrMap(String... elems) {
    Map<String, String> map = new HashMap<String, String>(elems.length);
    for (int i = 0; i < elems.length - 1; i += 2) {
      map.put(elems[i], elems[i + 1]);
    }
    return map;
  }

  /**
   * Runs the utility test.
   */
  private static int runUtility() {
    STDOUT.printf("Running utility tests:\n");
    check(Utility.VERSION.length() >= 3);
    check(Utility.OS_NAME.length() > 0);
    if (Utility.OS_NAME.equals("Linux")) {
      check(Utility.getMemoryCapacity() > 0);
      check(Utility.getMemoryUsage() > 0);
    }
    check(Utility.PAGE_SIZE > 0);
    Map params = Utility.parseParams("key1=value1,key2=value2,foo,");
    check(params.size() == 2);
    check(params.get("key1").equals("value1"));
    check(params.get("key2").equals("value2"));
    check(Utility.primaryHash("abc".getBytes(), (1L << 32) - 1) == 3042090208L);
    check(Utility.primaryHash("abc".getBytes(), 0) == -1472843703697547994L);
    check(Utility.secondaryHash("abc".getBytes(), (1L << 32) - 1) == 702176507L);
    check(Utility.secondaryHash("abc".getBytes(), 0) == 1765794342254572867L);
    check(Utility.editDistanceLev("", "") == 0);
    check(Utility.editDistanceLev("ac", "abc") == 1);
    check(Utility.editDistanceLev("あいう", "あう") == 1);
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the status test.
   */
  private static int runStatus() {
    STDOUT.printf("Running status tests:\n");
    Status status = new Status();
    check(status.equals(new Status()));
    check(status.getCode() == Status.SUCCESS);
    check(status.getMessage().length() == 0);
    check(status.toString().equals("SUCCESS"));
    check(status.isOK());
    status.set(Status.UNKNOWN_ERROR, "foobar");
    check(status.getCode() == Status.UNKNOWN_ERROR);
    check(status.getMessage().equals("foobar"));
    check(status.toString().equals("UNKNOWN_ERROR: foobar"));
    check(status.equals(new Status(Status.UNKNOWN_ERROR, "")));
    check(status.equals(Status.UNKNOWN_ERROR));
    check(!status.isOK());
    Status s2 = new Status(Status.NOT_IMPLEMENTED_ERROR, "void");
    status.join(s2);
    check(status.toString().equals("UNKNOWN_ERROR: foobar"));
    status.set(Status.SUCCESS, "OK");
    status.join(s2);
    check(status.toString().equals("NOT_IMPLEMENTED_ERROR: void"));
    try {
      status.orDie();
      check(false);
    } catch (StatusException e) {
      check(e.getStatus().equals(status));
    }
    status.set(Status.SUCCESS, "OK");
    status.orDie();
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the basic test.
   */
  private static int runBasic(String tmp_dir_path) {
    STDOUT.printf("Running basic tests:\n");
    ArrayList<Map<String, String>> confs = new ArrayList<Map<String, String>>();
    confs.add(Map.of(
        "path", "casket.tkh",
        "open_params",
        "update_mode=UPDATE_APPENDING,offset_width=3,align_pow=1,num_buckets=100,fbp_capacity=64",
        "rebuild_params",
        "update_mode=UPDATE_APPENDING,offset_width=3,align_pow=1,num_buckets=100,fbp_capacity=64",
        "synchronize_params", "",
        "expected_class", "HashDBM"));
    confs.add(Map.of(
        "path", "casket.tkt",
        "open_params",
        "update_mode=UPDATE_APPENDING,offset_width=3,align_pow=1,num_buckets=100,fbp_capacity=64" +
        ",max_page_size=100,max_branches=4,max_cached_pages=1,key_comparator=decimal",
        "rebuild_params",
        "update_mode=UPDATE_APPENDING,offset_width=3,align_pow=1,num_buckets=100,fbp_capacity=64" +
        ",max_page_size=100,max_branches=4,max_cached_pages=1",
        "synchronize_params", "",
        "expected_class", "TreeDBM"));
    confs.add(Map.of(
        "path", "casket.tks",
        "open_params",
        "offset_width=3,step_unit=2,max_level=3,sort_mem_size=100,insert_in_order=false" +
        ",max_cached_records=8",
        "rebuild_params",
        "offset_width=3,step_unit=2,max_level=3,sort_mem_size=100,max_cached_records=8",
        "synchronize_params", "reducer=last",
        "expected_class", "SkipDBM"));
    confs.add(Map.of(
        "path", "casket.tiny",
        "open_params", "num_buckets=10",
        "rebuild_params", "num_buckets=10",
        "synchronize_params", "",
        "expected_class", "TinyDBM"));
    confs.add(Map.of(
        "path", "casket.baby",
        "open_params", "key_comparator=decimal",
        "rebuild_params", "",
        "synchronize_params", "",
        "expected_class", "BabyDBM"));
    confs.add(Map.of(
        "path", "casket.cache",
        "open_params", "cap_rec_num=10000,cap_mem_size=10000000",
        "rebuild_params", "cap_rec_num=10000",
        "synchronize_params", "",
        "expected_class", "CacheDBM"));
    confs.add(Map.of(
        "path", "casket.stdhash",
        "open_params", "num_buckets=10",
        "rebuild_params", "",
        "synchronize_params", "",
        "expected_class", "StdHashDBM"));
    confs.add(Map.of(
        "path", "casket.stdtree",
        "open_params", "",
        "rebuild_params", "",
        "synchronize_params", "",
        "expected_class", "StdTreeDBM"));
    confs.add(Map.of(
        "path", "casket",
        "open_params", "num_shards=4,dbm=hash,num_buckets=100",
        "rebuild_params", "",
        "synchronize_params", "",
        "expected_class", "HashDBM"));
    for (Map<String, String> conf : confs) {
      String path = conf.get("path");
      if (!path.isEmpty()) {
        path = tmp_dir_path + java.io.File.separatorChar + path;
      }
      Map<String, String> open_params = Utility.parseParams(conf.get("open_params"));
      Map<String, String> rebuild_params = Utility.parseParams(conf.get("rebuild_params"));
      Map<String, String> synchronize_params = Utility.parseParams(conf.get("synchronize_params"));
      String expected_class = conf.get("expected_class");
      DBM dbm = new DBM();
      open_params.put("truncate", "true");
      check(!dbm.isOpen());
      check(dbm.open(path, true, open_params).equals(Status.SUCCESS));
      check(dbm.isOpen());
      Map<String, String> inspect = dbm.inspect();
      String class_name = inspect.get("class");
      check(class_name.equals(conf.get("expected_class")));
      for (int i = 0; i < 20; i++) {
        String key = String.format("%08d", i);
        String value = String.format("%d", i);
        if (i % 3 == 0) {
          check(dbm.set(key, value, false).equals(Status.SUCCESS));
        } else {
          check(dbm.set(key.getBytes(), value.getBytes(), false).equals(Status.SUCCESS));
        }
      }
      for (int i = 0; i < 20; i += 2) {
        String key = String.format("%08d", i);
        if (i % 3 == 0) {
          check(dbm.remove(key).equals(Status.SUCCESS));
        } else {
          check(dbm.remove(key.getBytes()).equals(Status.SUCCESS));
        }
      }
      if (class_name.equals("HashDBM") || class_name.equals("TreeDBM") ||
          class_name.equals("TinyDBM") || class_name.equals("BabyDBM")) {
        check(dbm.set("日本", "東京", true).equals(Status.SUCCESS));
        check(dbm.get("日本").equals("東京"));
        check(dbm.remove("日本").equals(Status.SUCCESS));
      }
      check(dbm.synchronize(false, synchronize_params).equals(Status.SUCCESS));
      check(dbm.count() == 10);
      check(dbm.getFileSize() > 0);
      if (!path.isEmpty()) {
        check(dbm.getFilePath().indexOf(path) >= 0);
        check(dbm.getTimestamp() > 0);
      }
      check(dbm.isWritable());
      check(dbm.isHealthy());
      if (class_name.equals("TreeDBM") || class_name.equals("SkipDBM") ||
          class_name.equals("BabyDBM") || class_name.equals("StdTreeDBM")) {
        check(dbm.isOrdered());
      } else {
        check(!dbm.isOrdered());
      }
      check(dbm.toString().indexOf(class_name) >= 0);
      for (int i = 0; i < 20; i++) {
        String key = String.format("%08d", i);
        String value = String.format("new-%d", i);
        Status status = dbm.set(key, value, false);
        if (i % 2 == 0) {
          check(status.equals(Status.SUCCESS));
        } else {
          check(status.equals(Status.DUPLICATION_ERROR));
        }
      }
      Status.And<String> sv = dbm.setAndGet("98765", "apple", false);
      check(sv.status.equals(Status.SUCCESS));
      check(sv.value == null);
      if (class_name.equals("HashDBM") || class_name.equals("TreeDBM") ||
          class_name.equals("TinyDBM") || class_name.equals("BabyDBM")) {
        sv = dbm.setAndGet("98765", "orange", false);
        check(sv.status.equals(Status.DUPLICATION_ERROR));
        check(sv.value.equals("apple"));
        sv = dbm.setAndGet("98765", "orange", true);
        check(sv.status.equals(Status.SUCCESS));
        check(sv.value.equals("apple"));
        check(dbm.get("98765").equals("orange"));
        sv = dbm.removeAndGet("98765");
        check(sv.status.equals(Status.SUCCESS));
        check(sv.value.equals("orange"));
        sv = dbm.removeAndGet("98765");
        check(sv.status.equals(Status.NOT_FOUND_ERROR));
        check(sv.value == null);
        check(dbm.set("98765", "banana").equals(Status.SUCCESS));
      }
      check(dbm.remove("98765").equals(Status.SUCCESS));
      check(dbm.synchronize(false, synchronize_params).equals(Status.SUCCESS));
      HashMap<String, String> records = new HashMap<String, String>();
      for (int i = 0; i < 20; i++) {
        String key = String.format("%08d", i);
        String value = i % 2 == 0 ? String.format("new-%d", i) : String.format("%d", i);
        check(dbm.get(key).equals(value));
        check(Arrays.equals(dbm.get(key.getBytes()), value.getBytes()));
        Status status = new Status();
        String rec_value = dbm.get(key, status);
        check(rec_value.equals(value));
        check(status.equals(Status.SUCCESS));
        records.put(key, value);
      }
      check(dbm.rebuild(rebuild_params).equals(Status.SUCCESS));
      HashMap<String, String> iter_records = new HashMap<String, String>();
      Iterator iter = dbm.makeIterator();
      Status status = new Status();
      byte[][] record_raw = iter.get(status);
      check(status.equals(Status.NOT_FOUND_ERROR));
      check(record_raw == null);
      String[] record = iter.getString(status);
      check(status.equals(Status.NOT_FOUND_ERROR));
      check(record == null);
      check(iter.first().equals(Status.SUCCESS));
      check(iter.toString().indexOf("0000") >= 0);
      while (true) {
        record = iter.getString(status);
        if (!status.equals(Status.SUCCESS)) {
          check(status.equals(Status.NOT_FOUND_ERROR));
          check(record == null);
          break;
        }
        check(record.length == 2);
        iter_records.put(record[0], record[1]);
        check(iter.next().equals(Status.SUCCESS));
      }
      check(iter_records.equals(records));
      check(iter.jump("00000011").equals(Status.SUCCESS));
      check(iter.getKeyString().equals("00000011"));
      check(iter.getValueString().equals("11"));
      if (dbm.isOrdered()) {
        check(iter.last().equals(Status.SUCCESS));
        iter_records = new HashMap<String, String>();
        while (true) {
          status = new Status();
          record = iter.getString(status);
          if (!status.equals(Status.SUCCESS)) {
            check(status.equals(Status.NOT_FOUND_ERROR));
            check(record == null);
            break;
          }
          iter_records.put(record[0], record[1]);
          check(iter.previous().equals(Status.SUCCESS));
        }
      }
      check(iter_records.equals(records));
      if (!path.isEmpty()) {
        String[] parts = path.split("\\.(?=[^\\.]+$)");
        String copy_path = parts[0] + "-copy";
        if (parts.length > 1) {
          copy_path += "." + parts[1];
        }
        check(dbm.copyFileData(copy_path, false).equals(Status.SUCCESS));
        DBM copy_dbm = new DBM();
        if (path.indexOf(".") >= 0) {
          check(copy_dbm.open(copy_path, false).equals(Status.SUCCESS));
        } else {
          HashMap<String, String> params = new HashMap<String, String>();
          params.put("dbm", expected_class);
          if (open_params.containsKey("num_shards")) {
            params.put("num_shards", "0");
          }
          check(copy_dbm.open(copy_path, false, params).equals(Status.SUCCESS));
        }
        check(copy_dbm.isHealthy());
        check(copy_dbm.count() == records.size());
        check(copy_dbm.close().equals(Status.SUCCESS));
        copy_dbm.destruct();
        if (class_name.equals("HashDBM") || class_name.equals("TreeDBM")) {
          String restored_path = copy_path + "-restored";
          check(DBM.restoreDatabase(
              copy_path, restored_path, class_name, -1).equals(Status.SUCCESS));
        }
      }
      DBM export_dbm = new DBM();
      check(export_dbm.open("", true, Utility.parseParams("dbm=BabyDBM")).equals(Status.SUCCESS));
      check(dbm.export(export_dbm).equals(Status.SUCCESS));
      HashMap<String, String> export_records = new HashMap<String, String>();
      Iterator export_iter = export_dbm.makeIterator();
      check(export_iter.first().equals(Status.SUCCESS));
      while (true) {
        record = export_iter.getString();
        if (record == null) {
          break;
        }
        export_records.put(record[0], record[1]);
        export_iter.next();
      }
      check(export_records.equals(records));
      check(export_dbm.clear().equals(Status.SUCCESS));
      check(export_dbm.count() == 0);
      check(export_dbm.set("1", "100").equals(Status.SUCCESS));
      long cur_value = export_dbm.increment("10000", 2, 10000, status);
      check(status.equals(Status.SUCCESS));
      check(cur_value == 10002);
      check(export_dbm.set("1", "101", false).equals(Status.DUPLICATION_ERROR));
      check(export_dbm.compareExchange("1", "100", "101").equals(Status.SUCCESS));
      cur_value = export_dbm.increment("10000", 2, 0, status);
      check(status.equals(Status.SUCCESS));
      check(cur_value == 10004);
      cur_value = export_dbm.increment("10000", Long.MIN_VALUE, 0, status);
      check(status.equals(Status.SUCCESS));
      check(cur_value == 10004);
      check(export_dbm.compareExchange("1", "100", "101").equals(Status.INFEASIBLE_ERROR));
      check(export_dbm.compareExchange("1", "101", null).equals(Status.SUCCESS));
      String value = export_dbm.get("1", status);
      check(status.equals(Status.NOT_FOUND_ERROR));
      check(export_dbm.compareExchange("1", null, "zzz").equals(Status.SUCCESS));
      check(export_dbm.compareExchange("1", null, "yyy").equals(Status.INFEASIBLE_ERROR));
      check(export_dbm.get("1").equals("zzz"));
      check(export_dbm.compareExchange("1", "zzz", null).equals(Status.SUCCESS));
      check(export_iter.first().equals(Status.SUCCESS));
      check(export_iter.set("foobar").equals(Status.SUCCESS));
      check(export_iter.remove().equals(Status.SUCCESS));
      check(export_dbm.compareExchangeMultiStr(
          makeStrMap("hop", null, "step", null),
          makeStrMap("hop", "one", "step", "two")).equals(Status.SUCCESS));
      check(export_dbm.get("hop").equals("one"));
      check(export_dbm.get("step").equals("two"));
      check(export_dbm.compareExchangeMultiStr(
          makeStrMap("hop", "one", "step", null),
          makeStrMap("hop", "uno", "step", "dos")).equals(Status.INFEASIBLE_ERROR));
      check(export_dbm.get("hop").equals("one"));
      check(export_dbm.get("step").equals("two"));
      check(export_dbm.compareExchangeMultiStr(
          makeStrMap("hop", "one", "step", "two"),
          makeStrMap("hop", "1", "step", "2")).equals(Status.SUCCESS));
      check(export_dbm.get("hop").equals("1"));
      check(export_dbm.get("step").equals("2"));
      check(export_dbm.compareExchangeMultiStr(
          makeStrMap("hop", "1", "step", "2"),
          makeStrMap("hop", null, "step", null)).equals(Status.SUCCESS));
      check(export_dbm.get("hop") == null);
      check(export_dbm.get("step") == null);
      check(export_dbm.count() == 0);
      check(export_dbm.append("foo", "bar", ",").equals(Status.SUCCESS));
      check(export_dbm.append("foo", "baz", ",").equals(Status.SUCCESS));
      check(export_dbm.append("foo", "qux", "").equals(Status.SUCCESS));
      check(export_dbm.get("foo").equals("bar,bazqux"));
      Map<String, String> multi_records = Map.of(
          "one", "first", "two", "second", "three", "third");
      check(export_dbm.setMultiStr(multi_records, true).equals(Status.SUCCESS));
      multi_records = Map.of("two", "2", "three", "3");
      check(export_dbm.appendMulti(multi_records, ":").equals(Status.SUCCESS));
      String[] multi_keys = {"one", "two", "three", "four"};
      multi_records = export_dbm.getMulti(multi_keys);
      check(multi_records.get("one").equals("first"));
      check(multi_records.get("two").equals("second:2"));
      check(multi_records.get("three").equals("third:3"));
      String[] half_keys = {"one", "two"};
      check(export_dbm.removeMulti(half_keys).equals(Status.SUCCESS));
      check(export_dbm.get("one", status) == null);
      check(status.equals(Status.NOT_FOUND_ERROR));
      check(export_dbm.get("two", status) == null);
      check(status.equals(Status.NOT_FOUND_ERROR));
      check(export_dbm.removeMulti(multi_keys).equals(Status.NOT_FOUND_ERROR));
      check(export_dbm.get("three", status) == null);
      check(status.equals(Status.NOT_FOUND_ERROR));
      check(export_dbm.get("four", status) == null);
      check(status.equals(Status.NOT_FOUND_ERROR));
      Map<byte[], byte[]> raw_multi_records =
          Map.of("one".getBytes(), "first".getBytes(),
                 "two".getBytes(), "second".getBytes(),
                 "three".getBytes(), "third".getBytes());
      check(export_dbm.setMulti(raw_multi_records, false).equals(Status.SUCCESS));
      raw_multi_records =
          Map.of("two".getBytes(), "2".getBytes(),
                 "three".getBytes(), "3".getBytes());
      check(export_dbm.appendMulti(raw_multi_records, ":".getBytes()).equals(Status.SUCCESS));
      byte[][] raw_multi_keys =
          {"one".getBytes(), "two".getBytes(), "three".getBytes(), "four".getBytes()};
      raw_multi_records = export_dbm.getMulti(raw_multi_keys);
      multi_records.clear();
      for (Map.Entry<byte[], byte[]> raw_record : raw_multi_records.entrySet()) {
        multi_records.put(new String(raw_record.getKey()), new String(raw_record.getValue()));
      }
      check(multi_records.get("one").equals("first"));
      check(multi_records.get("two").equals("second:2"));
      check(multi_records.get("three").equals("third:3"));
      byte[][] raw_multi_keys_remove =
          {"one".getBytes(), "two".getBytes(), "three".getBytes()};
      check(export_dbm.removeMulti(raw_multi_keys_remove).equals(Status.SUCCESS));
      check(export_dbm.count() == 1);
      check(export_dbm.set("zero", "foo").equals(Status.SUCCESS));
      check(export_dbm.rekey("zero", "one", true).equals(Status.SUCCESS));
      check(export_dbm.get("zero") == null);
      check(export_dbm.get("one").equals("foo"));
      int step_count = 0;
      check(export_iter.first().equals(Status.SUCCESS));
      while (true) {
        status.set(Status.UNKNOWN_ERROR, "");
        record = export_iter.stepString(status);
        if (record == null) {
          check(status.equals(Status.NOT_FOUND_ERROR));
          break;
        }
        check(status.equals(Status.SUCCESS));
        step_count++;
      }
      check(step_count == export_dbm.count());
      int pop_count = 0;
      while (true) {
        status.set(Status.UNKNOWN_ERROR, "");
        record = export_iter.popFirstString(status);
        if (record == null) {
          check(status.equals(Status.NOT_FOUND_ERROR));
          break;
        }
        check(status.equals(Status.SUCCESS));
        pop_count++;
      }
      check(pop_count == step_count);
      check(export_dbm.count() == 0);
      export_iter.destruct();
      check(export_dbm.close().equals(Status.SUCCESS));
      export_dbm.destruct();
      iter.destruct();
      check(dbm.close().equals(Status.Code.SUCCESS));
      dbm.destruct();
    }
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the iterator test.
   */
  private static int runIter(String tmp_dir_path) {
    STDOUT.printf("Running iterator tests:\n");
    ArrayList<Map<String, String>> confs = new ArrayList<Map<String, String>>();
    confs.add(Map.of(
        "path", "casket.tkt",
        "open_params", "num_buckets=100,max_page_size=10,max_branches=2"));
    confs.add(Map.of(
        "path", "casket.tks",
        "open_params", "step_unit=3,max_level=3"));
    confs.add(Map.of(
        "path", "casket.tkt",
        "open_params", "num_shards=4,num_buckets=100,max_page_size=10,max_branches=2"));
    confs.add(Map.of(
        "path", "",
        "open_params", "num_shards=4,dbm=baby"));
    for (Map<String, String> conf : confs) {
      String path = conf.get("path");
      if (!path.isEmpty()) {
        path = tmp_dir_path + java.io.File.separatorChar + path;
      }
      Map<String, String> open_params = Utility.parseParams(conf.get("open_params"));
      DBM dbm = new DBM();
      open_params.put("truncate", "true");
      check(dbm.open(path, true, open_params).equals(Status.SUCCESS));
      Iterator iter = dbm.makeIterator();
      check(iter.first().equals(Status.SUCCESS));
      check(iter.last().equals(Status.SUCCESS));
      check(iter.jump("").equals(Status.SUCCESS));
      check(iter.jumpLower("", true).equals(Status.SUCCESS));
      check(iter.jumpUpper("", true).equals(Status.SUCCESS));
      for (int i = 1; i <= 100; i++) {
        String key = String.format("%03d", i);
        String value = String.format("%d", i * i);
        check(dbm.set(key, value, false).equals(Status.SUCCESS));
      }
      check(dbm.synchronize(false).equals(Status.SUCCESS));
      check(iter.first().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("001"));
      check(iter.getValueString().equals("1"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("002"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("001"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString() == null);
      check(iter.last().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("100"));
      check(iter.getValueString().equals("10000"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("099"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("100"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString() == null);
      check(iter.jump("050").equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("051"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.jump("051").equals(Status.SUCCESS));
      check(iter.getKeyString().equals("051"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("051"));
      check(iter.jumpLower("050", true).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("049"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.jumpLower("050", false).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("049"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("049"));
      check(iter.jumpLower("001", true).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("001"));
      check(iter.jumpLower("001", false).equals(Status.SUCCESS));
      check(iter.getKeyString() == null);
      check(iter.jumpLower("101", true).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("100"));
      check(iter.jumpUpper("050", true).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("051"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.jumpUpper("050", false).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("051"));
      check(iter.previous().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("050"));
      check(iter.next().equals(Status.SUCCESS));
      check(iter.getKeyString().equals("051"));
      check(iter.jumpUpper("100", true).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("100"));
      check(iter.jumpUpper("100", false).equals(Status.SUCCESS));
      check(iter.getKeyString() == null);
      check(iter.jumpUpper("000", true).equals(Status.SUCCESS));
      check(iter.getKeyString().equals("001"));
      check(iter.jumpLower("051", false).equals(Status.SUCCESS));
      int count = 0;
      while (true) {
        byte[][] record = iter.get();
        if (record == null) {
          break;
        }
        count++;
        check(iter.previous().equals(Status.SUCCESS));
      }
      check(count == 50);
      check(iter.jumpUpper("050", false).equals(Status.SUCCESS));
      count = 0;
      while (true) {
        byte[][] record = iter.get();
        if (record == null) {
          break;
        }
        count++;
        check(iter.next().equals(Status.SUCCESS));
      }
      check(count == 50);
      iter.destruct();
      check(dbm.close().equals(Status.Code.SUCCESS));
      dbm.destruct();
    }
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the threadc test.
   */
  private static int runThread(String tmp_dir_path) {
    STDOUT.printf("Running thread tests:\n");
    String path = tmp_dir_path + java.io.File.separatorChar + "casket.tkh";
    DBM dbm = new DBM();
    check(dbm.open(path, true, Utility.parseParams("num_buckets=1000")).equals(Status.SUCCESS));
    int num_records = 5000;
    int num_threads = 5;
    class Task extends Thread {
      public Task(int thid) {
        thid_ = thid;
        records_ = new HashMap<String, String>();
      }
      public void run() {
        Random rnd = new Random(thid_);
        for (int i = 0; i < num_records; i++) {
          int key_num = rnd.nextInt(num_records);
          key_num = key_num - key_num % num_threads + thid_;
          String key = String.format("%d", key_num);
          String value = String.format("%d", key_num * key_num);
          if (rnd.nextInt(num_records) == 0) {
            check(dbm.rebuild().equals(Status.SUCCESS));
          } else if (rnd.nextInt(10) == 0) {
            Iterator iter = dbm.makeIterator();
            iter.jump(key);
            Status status = new Status();
            String[] record = iter.getString(status);
            if (status.equals(Status.SUCCESS)) {
              check(record[0].equals(key));
              check(record[1].equals(value));
              iter.next();
            }
            iter.destruct();
          } else if (rnd.nextInt(4) == 0) {
            Status status = new Status();
            String rec_value = dbm.get(key, status);
            if (status.equals(Status.SUCCESS)) {
              check(rec_value.equals(value));
            } else {
              status.equals(Status.NOT_FOUND_ERROR);
            }
          } else if (rnd.nextInt(4) == 0) {
            Status status = dbm.remove(key);
            if (status.equals(Status.SUCCESS)) {
              records_.remove(key);
            } else {
              status.equals(Status.NOT_FOUND_ERROR);
            }
          } else {
            boolean overwrite = rnd.nextInt(2) == 0;
            Status status = dbm.set(key, value, overwrite);
            if (status.equals(Status.SUCCESS)) {
              records_.put(key, value);
            } else {
              status.equals(Status.DUPLICATION_ERROR);
            }
          }
        }
      }
      private int thid_;
      public HashMap<String, String> records_;
    }
    Task[] tasks = new Task[num_threads];
    for (int thid = 0; thid < num_threads; thid++) {
      tasks[thid] = new Task(thid);
      tasks[thid].start();
    }
    HashMap<String, String> records = new HashMap<String, String>();
    for (int thid = 0; thid < num_threads; thid++) {
      try {
        tasks[thid].join();
      } catch (java.lang.InterruptedException e) {
        throw new RuntimeException(e);
      }
      for (Map.Entry<String, String> entry : tasks[thid].records_.entrySet()) {
        records.put(entry.getKey(), entry.getValue());
      }
    }
    check(records.size() == dbm.count());
    for (Map.Entry<String, String> entry : records.entrySet()) {
      Status status = new Status();
      String value = dbm.get(entry.getKey(), status);
      check(status.equals(Status.SUCCESS));
      check(value.equals(entry.getValue()));
    }
    check(dbm.close().equals(Status.SUCCESS));
    dbm.destruct();
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the search test.
   */
  private static int runSearch(String tmp_dir_path) {
    STDOUT.printf("Running search tests:\n");
    ArrayList<Map<String, String>> confs = new ArrayList<Map<String, String>>();
    confs.add(Map.of(
        "path", "casket.tkh",
        "open_params", "num_buckets=100"));
    confs.add(Map.of(
        "path", "casket.tkt",
        "open_params", "num_buckets=100"));
    confs.add(Map.of(
        "path", "casket.tks",
        "open_params", "max_level=8"));
    confs.add(Map.of(
        "path", "",
        "open_params", "dbm=TinyDBM,num_buckets=100"));
    confs.add(Map.of(
        "path", "",
        "open_params", "dbm=BabyDBM"));
    for (Map<String, String> conf : confs) {
      String path = conf.get("path");
      if (!path.isEmpty()) {
        path = tmp_dir_path + java.io.File.separatorChar + path;
      }
      Map<String, String> open_params = Utility.parseParams(conf.get("open_params"));
      DBM dbm = new DBM();
      open_params.put("truncate", "true");
      check(dbm.open(path, true, open_params).equals(Status.SUCCESS));
      Map<String, String> inspect = dbm.inspect();
      for (int i = 1; i <= 100; i++) {
        String key = String.format("%08d", i);
        String value = String.format("%d", i);
        check(dbm.set(key, value, false).equals(Status.SUCCESS));
      }
      check(dbm.synchronize(false).equals(Status.SUCCESS));
      check(dbm.count() == 100);
      check(dbm.search("contain", "001", 0).length == 12);
      check(dbm.search("contain", "001", 3).length == 3);
      check(dbm.search("begin", "0000001", 0).length == 10);
      check(dbm.search("end", "1", 0).length == 10);
      check(dbm.search("regex", "^\\d+1$", 0).length == 10);
      check(dbm.search("edit", "00000100", 3).length == 3);
      check(dbm.search("editbin", "00000100", 3).length == 3);
      try {
        dbm.search("foo", "00000100", 3);
        check(false);
      } catch (StatusException e) {
        check(e.getStatus().equals(Status.Code.INVALID_ARGUMENT_ERROR));
      }
      check(dbm.close().equals(Status.Code.SUCCESS));
      dbm.destruct();
    }
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the export test.
   */
  private static int runExport(String tmp_dir_path) {
    STDOUT.printf("Running export tests:\n");
    String path = tmp_dir_path + java.io.File.separatorChar + "casket.tkh";
    String dest_path = tmp_dir_path + java.io.File.separatorChar + "casket.txt";
    DBM dbm = new DBM();
    check(dbm.open(path, true, "num_buckets=100").equals(Status.SUCCESS));
    for (int i = 1; i <= 100; i++) {
      String key = String.format("%08d", i);
      String value = String.format("%d", i);
      check(dbm.set(key, value, false).equals(Status.SUCCESS));
    }
    File file = new File();
    check(file.open(dest_path, true, "truncate=true").equals(Status.SUCCESS));
    check(dbm.exportToFlatRecords(file).equals(Status.Code.SUCCESS));
    check(dbm.clear().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    check(dbm.importFromFlatRecords(file).equals(Status.Code.SUCCESS));
    check(dbm.count() == 100);
    check(file.close().equals(Status.SUCCESS));
    file.destruct();
    file = new File();
    check(file.open(dest_path, true, "truncate=true").equals(Status.SUCCESS));
    check(dbm.exportKeysAsLines(file).equals(Status.Code.SUCCESS));
    check(file.close().equals(Status.SUCCESS));
    file.destruct();
    check(dbm.close().equals(Status.SUCCESS));
    dbm.destruct();
    file = new File();
    check(file.open(dest_path, false).equals(Status.SUCCESS));
    check(file.toString().indexOf("File") >= 0);
    check(file.search("contain", "001", 0).length == 12);
    check(file.search("contain", "001", 3).length == 3);
    check(file.search("begin", "0000001", 0).length == 10);
    check(file.search("end", "1", 0).length == 10);
    check(file.search("regex", "^\\d+1$", 0).length == 10);
    check(file.search("edit", "00000100", 3).length == 3);
    check(file.search("editbin", "00000100", 3).length == 3);
    try {
      file.search("foo", "00000100", 3);
      check(false);
    } catch (StatusException e) {
      check(e.getStatus().equals(Status.Code.INVALID_ARGUMENT_ERROR));
    }
    check(file.close().equals(Status.Code.SUCCESS));
    file.destruct();
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the export test.
   */
  private static int runAsyncDBM(String tmp_dir_path) {
    STDOUT.printf("Running export tests:\n");
    String path = tmp_dir_path + java.io.File.separatorChar + "casket.tkh";
    String copy_path = tmp_dir_path + java.io.File.separatorChar + "casket-copy.tkh";
    DBM dbm = new DBM();
    check(dbm.open(path, true, "num_buckets=100").equals(Status.SUCCESS));
    AsyncDBM async = new AsyncDBM(dbm, 4);
    check(async.toString().indexOf("AsyncDBM") >= 0);
    Future<Status> future = async.set("one", "hop", true);
    check(future.toString().indexOf("Future") >= 0);
    future.await(0);
    check(future.await(-1));
    check(future.get().equals(Status.SUCCESS));
    check(async.set("two".getBytes(), "step".getBytes(), true).get().equals(Status.SUCCESS));
    check(async.set("three".getBytes(), "jump".getBytes()).get().equals(Status.SUCCESS));
    check(dbm.count() == 3);
    check(async.append("one".getBytes(), "1".getBytes(), ":".getBytes())
          .get().equals(Status.SUCCESS));
    check(async.append("two", "2", ":").get().equals(Status.SUCCESS));
    Future<Status.And<String>> get_str_future = async.get("one");
    Status.And<String> get_str_result = get_str_future.get();
    check(get_str_result.status.equals(Status.SUCCESS));
    check(get_str_result.value.equals("hop:1"));
    Future<Status.And<byte[]>> get_raw_future = async.get("two".getBytes());
    Status.And<byte[]> get_raw_result = get_raw_future.get();
    check(get_raw_result.status.equals(Status.SUCCESS));
    check(Arrays.equals(get_raw_result.value, "step:2".getBytes()));
    future = async.set("one", "hop", true);
    check(future.toString().indexOf("Future") >= 0);
    future.await(0);
    check(future.await(-1));
    check(future.get().equals(Status.SUCCESS));
    check(async.set("two".getBytes(), "hop".getBytes(), true).get().equals(Status.SUCCESS));
    check(async.set("three".getBytes(), "jump".getBytes()).get().equals(Status.SUCCESS));
    check(dbm.count() == 3);
    check(async.remove("one").get().equals(Status.SUCCESS));
    check(async.remove("two".getBytes()).get().equals(Status.SUCCESS));
    check(async.remove("three".getBytes()).get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    Map<String, String> str_records = Map.of("one", "hop", "two", "step", "three", "jump");
    check(async.setMultiStr(str_records, false).get().equals(Status.SUCCESS));
    check(dbm.count() == 3);
    str_records = Map.of("one", "1", "two", "2", "three", "3");
    check(async.appendMulti(str_records, ":").get().equals(Status.SUCCESS));
    String[] str_keys = {"one", "two", "three"};
    Future<Status.And<Map<String, String>>> get_multi_str_future = async.getMulti(str_keys);
    Status.And<Map<String, String>> get_multi_str_result = get_multi_str_future.get();
    check(get_multi_str_result.status.equals(Status.SUCCESS));
    str_records = get_multi_str_result.value;
    check(str_records.size() == 3);
    check(str_records.get("one").equals("hop:1"));
    check(str_records.get("two").equals("step:2"));
    check(str_records.get("three").equals("jump:3"));
    check(async.removeMulti(str_keys).get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    Map<byte[], byte[]> raw_records = Map.of(
        "one".getBytes(), "hop".getBytes(), "two".getBytes(), "step".getBytes(),
        "three".getBytes(), "jump".getBytes());
    check(async.setMulti(raw_records, false).get().equals(Status.SUCCESS));
    check(dbm.count() == 3);
    raw_records = Map.of(
        "one".getBytes(), "1".getBytes(), "two".getBytes(), "2".getBytes(),
        "three".getBytes(), "3".getBytes());
    check(async.appendMulti(raw_records, ":".getBytes()).get().equals(Status.SUCCESS));
    byte[][] raw_keys = {"one".getBytes(), "two".getBytes(), "three".getBytes()};
    Future<Status.And<Map<byte[], byte[]>>> get_multi_raw_future = async.getMulti(raw_keys);
    Status.And<Map<byte[], byte[]>> get_multi_raw_result = get_multi_raw_future.get();
    check(get_multi_raw_result.status.equals(Status.SUCCESS));
    str_records.clear();
    for (Map.Entry<byte[], byte[]> raw_record : get_multi_raw_result.value.entrySet()) {
      str_records.put(new String(raw_record.getKey()), new String(raw_record.getValue()));
    }
    check(str_records.size() == 3);
    check(str_records.get("one").equals("hop:1"));
    check(str_records.get("two").equals("step:2"));
    check(str_records.get("three").equals("jump:3"));
    check(async.removeMulti(raw_keys).get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    check(async.compareExchange("japan", null, "tokyo").get().equals(Status.SUCCESS));
    check(async.compareExchange("japan", "tokyo", "kyoto").get().equals(Status.SUCCESS));
    check(dbm.get("japan").equals("kyoto"));
    check(async.compareExchange("japan", "kyoto", null).get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    check(async.compareExchangeMultiStr(
        makeStrMap("one", null, "two", null),
        makeStrMap("one", "1", "two", "2")).get().equals(Status.SUCCESS));
    check(async.compareExchangeMultiStr(
        makeStrMap("one", "1", "two", "2"),
        makeStrMap("one", "11", "two", "22")).get().equals(Status.SUCCESS));
    check(dbm.get("one").equals("11"));
    check(dbm.get("two").equals("22"));
    check(async.compareExchangeMultiStr(
        makeStrMap("one", "11", "two", "22"),
        makeStrMap("one", null, "two", null)).get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    Future<Status.And<Long>> incr_future = async.increment("num", 5, 100);
    Status.And<Long> incr_result = incr_future.get();
    check(incr_result.status.equals(Status.SUCCESS));
    check(incr_result.value.longValue() == 105);
    check(async.increment("num", 5, 100).get().value.longValue() == 110);
    check(async.rebuild().get().equals(Status.SUCCESS));
    check(async.synchronize(false).get().equals(Status.SUCCESS));
    check(async.copyFileData(copy_path, false).get().equals(Status.SUCCESS));
    check(async.clear().get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    DBM copy_dbm = new DBM();
    check(copy_dbm.open(copy_path, true).equals(Status.SUCCESS));
    check(copy_dbm.count() == 1);
    check(copy_dbm.clear().equals(Status.SUCCESS));
    check(dbm.set("tako", "ika").equals(Status.SUCCESS));
    check(async.export(copy_dbm).get().equals(Status.SUCCESS));
    check(copy_dbm.get("tako").equals("ika"));
    check(copy_dbm.close().equals(Status.SUCCESS));
    copy_dbm.destruct();
    File copy_file = new File();
    check(copy_file.open(copy_path, true, "truncate=true").equals(Status.SUCCESS));
    check(async.exportToFlatRecords(copy_file).get().equals(Status.SUCCESS));
    check(async.clear().get().equals(Status.SUCCESS));
    check(dbm.count() == 0);
    check(async.importFromFlatRecords(copy_file).get().equals(Status.SUCCESS));
    check(dbm.count() == 1);
    check(dbm.set("tako", "ika").equals(Status.SUCCESS));
    check(copy_file.close().equals(Status.SUCCESS));
    copy_file.destruct();
    check(dbm.set("hello", "bood-bye").equals(Status.SUCCESS));
    check(dbm.set("hi", "bye").equals(Status.SUCCESS));
    check(dbm.set("chao", "adios").equals(Status.SUCCESS));
    Future<Status.And<String[]>> search_str_future = async.search("begin", "h", 0);
    Status.And<String[]> search_str_result = search_str_future.get();
    check(search_str_result.status.equals(Status.SUCCESS));
    check(search_str_result.value.length == 2);
    Future<Status.And<byte[][]>> search_raw_future = async.search("begin", "h".getBytes(), 0);
    Status.And<byte[][]> search_raw_result = search_raw_future.get();
    check(search_raw_result.status.equals(Status.SUCCESS));
    check(search_raw_result.value.length == 2);
    async.destruct();
    check(dbm.close().equals(Status.Code.SUCCESS));
    dbm.destruct();
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the file test.
   */
  private static int runFile(String tmp_dir_path) {
    STDOUT.printf("Running file tests:\n");
    String path = tmp_dir_path + java.io.File.separatorChar + "casket.txt";
    File file = new File();
    Map<String, String> params = Utility.parseParams(
        "truncate=true,file=pos-atom,block_size=512,access_options=padding:pagecache");
    check(file.open(path, true, params).equals(Status.SUCCESS));
    check(file.write(5, "12345").equals(Status.SUCCESS));
    check(file.write(0, "ABCDE").equals(Status.SUCCESS));
    check(file.append("FGH") == 10);
    check(file.append("IJ") == 13);
    check(file.getSize() == 15);
    check(file.getPath().indexOf("casket.txt") > 0);
    check(file.truncate(12).equals(Status.SUCCESS));
    check(file.synchronize(false).equals(Status.SUCCESS));
    check(file.getSize() == 12);
    String str = file.readString(0, 12);
    check(str.equals("ABCDE12345FG"));
    str = file.readString(3, 5);
    check(str.equals("DE123"));
    Status status = new Status();
    check(file.read(1024, 10, status) == null);
    check(status.equals(Status.INFEASIBLE_ERROR));
    check(file.close().equals(Status.SUCCESS));
    check(file.open(path, false).equals(Status.SUCCESS));
    check(file.getSize() == 512);
    str = file.readString(4, 7);
    check(str.equals("E12345F"));
    check(file.close().equals(Status.SUCCESS));
    file.destruct();
    STDOUT.printf("  ... OK\n");
    return 0;
  }

  /**
   * Runs the perf test.
   */
  private static int runPerf(String path, int num_iterations, int num_threads,
                             String params, boolean is_random) {
    Map<String, String> params_map = Utility.parseParams(params);
    STDOUT.printf("path: %s\n", path);
    STDOUT.printf("params: %s\n", params_map);
    STDOUT.printf("num_iterations: %d\n", num_iterations);
    STDOUT.printf("num_threads: %d\n", num_threads);
    STDOUT.printf("is_random: %s\n", is_random);
    STDOUT.printf("\n");
    params_map.put("truncate", "true");
    System.gc();
    long start_mem_usage = Utility.getMemoryUsage();
    DBM dbm = new DBM();
    dbm.open(path, true, params_map).orDie();
    class Setter extends Thread {
      public Setter(int thid) {
        thid_ = thid;
      }
      public void run() {
        Random rnd = new Random(thid_);
        for (int i = 0; i < num_iterations; i++) {
          int key_num = 0;
          if (is_random) {
            key_num = rnd.nextInt(num_threads * num_iterations);
          } else {
            key_num = thid_ * num_iterations + i;
          }
          String key = String.format("%08d", key_num);
          dbm.set(key, key).orDie();
          int seq = i + 1;
          if (thid_ == 0 && seq % (num_iterations / 500) == 0) {
            STDOUT.printf(".");
            if (seq % (num_iterations / 10) == 0) {
              STDOUT.printf(" (%08d)\n", seq);
            }
            STDOUT.flush();
          }
        }
      }
      private int thid_;
    }
    STDOUT.printf("Setting:\n");
    double start_time = getTime();
    Setter[] setters = new Setter[num_threads];
    for (int thid = 0; thid < num_threads; thid++) {
      setters[thid] = new Setter(thid);
      setters[thid].start();
    }
    for (int thid = 0; thid < num_threads; thid++) {
      try {
        setters[thid].join();
      } catch (java.lang.InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    dbm.synchronize(false).orDie();
    double end_time = getTime();
    double elapsed = end_time - start_time;
    System.gc();
    long mem_usage = Utility.getMemoryUsage() - start_mem_usage;
    STDOUT.printf("Setting done: num_records=%d file_size=%d time=%.3f qps=%.0f mem=%d\n",
                  dbm.count(), dbm.getFileSize(), elapsed,
                  num_iterations * num_threads / elapsed, mem_usage);
    STDOUT.print("\n");
    class Getter extends Thread {
      public Getter(int thid) {
        thid_ = thid;
      }
      public void run() {
        Random rnd = new Random(thid_);
        for (int i = 0; i < num_iterations; i++) {
          int key_num = 0;
          if (is_random) {
            key_num = rnd.nextInt(num_threads * num_iterations);
          } else {
            key_num = thid_ * num_iterations + i;
          }
          String key = String.format("%08d", key_num);
          Status status = new Status();
          dbm.get(key, status);
          if (!status.equals(Status.NOT_FOUND_ERROR)) {
            status.orDie();
          }
          int seq = i + 1;
          if (thid_ == 0 && seq % (num_iterations / 500) == 0) {
            STDOUT.printf(".");
            if (seq % (num_iterations / 10) == 0) {
              STDOUT.printf(" (%08d)\n", seq);
            }
            STDOUT.flush();
          }
        }
      }
      private int thid_;
    }
    STDOUT.printf("Getting:\n");
    start_time = getTime();
    Getter[] getters = new Getter[num_threads];
    for (int thid = 0; thid < num_threads; thid++) {
      getters[thid] = new Getter(thid);
      getters[thid].start();
    }
    for (int thid = 0; thid < num_threads; thid++) {
      try {
        getters[thid].join();
      } catch (java.lang.InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    end_time = getTime();
    elapsed = end_time - start_time;
    System.gc();
    mem_usage = Utility.getMemoryUsage() - start_mem_usage;
    STDOUT.printf("Getting done: num_records=%d file_size=%d time=%.3f qps=%.0f mem=%d\n",
                  dbm.count(), dbm.getFileSize(), elapsed,
                  num_iterations * num_threads / elapsed, mem_usage);
    STDOUT.print("\n");
    class Remover extends Thread {
      public Remover(int thid) {
        thid_ = thid;
      }
      public void run() {
        Random rnd = new Random(thid_);
        for (int i = 0; i < num_iterations; i++) {
          int key_num = 0;
          if (is_random) {
            key_num = rnd.nextInt(num_threads * num_iterations);
          } else {
            key_num = thid_ * num_iterations + i;
          }
          String key = String.format("%08d", key_num);
          Status status = dbm.remove(key);
          if (!status.equals(Status.NOT_FOUND_ERROR)) {
            status.orDie();
          }
          int seq = i + 1;
          if (thid_ == 0 && seq % (num_iterations / 500) == 0) {
            STDOUT.printf(".");
            if (seq % (num_iterations / 10) == 0) {
              STDOUT.printf(" (%08d)\n", seq);
            }
            STDOUT.flush();
          }
        }
      }
      private int thid_;
    }
    STDOUT.printf("Removing:\n");
    start_time = getTime();
    Remover[] removers = new Remover[num_threads];
    for (int thid = 0; thid < num_threads; thid++) {
      removers[thid] = new Remover(thid);
      removers[thid].start();
    }
    for (int thid = 0; thid < num_threads; thid++) {
      try {
        removers[thid].join();
      } catch (java.lang.InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    dbm.synchronize(false).orDie();
    end_time = getTime();
    elapsed = end_time - start_time;
    System.gc();
    mem_usage = Utility.getMemoryUsage() - start_mem_usage;
    STDOUT.printf("Removing done: num_records=%d file_size=%d time=%.3f qps=%.0f mem=%d\n",
                  dbm.count(), dbm.getFileSize(), elapsed,
                  num_iterations * num_threads / elapsed, mem_usage);
    STDOUT.print("\n");
    dbm.close().orDie();
    dbm.destruct();
    return 0;
  }

  /**
   * Runs the wicked test.
   */
  private static int runWicked(String path, int num_iterations, int num_threads,
                               String params) {
    Map<String, String> params_map = Utility.parseParams(params);
    STDOUT.printf("path: %s\n", path);
    STDOUT.printf("params: %s\n", params_map);
    STDOUT.printf("num_iterations: %d\n", num_iterations);
    STDOUT.printf("num_threads: %d\n", num_threads);
    STDOUT.printf("\n");
    params_map.put("truncate", "true");
    System.gc();
    long start_mem_usage = Utility.getMemoryUsage();
    DBM dbm = new DBM();
    dbm.open(path, true, params_map).orDie();
    class Task extends Thread {
      public Task(int thid) {
        thid_ = thid;
      }
      public void run() {
        Random rnd = new Random(thid_);
        for (int i = 0; i < num_iterations; i++) {
          int key_num = rnd.nextInt(num_threads * num_iterations);
          String key = String.format("%d", key_num);
          String value = String.format("%d", i);
          if (rnd.nextInt(num_iterations / 2) == 0) {
            dbm.rebuild().orDie();
          } else if (rnd.nextInt(num_iterations / 2) == 0) {
            dbm.clear().orDie();
          } else if (rnd.nextInt(num_iterations / 2) == 0) {
            dbm.synchronize(false).orDie();
          } else if (rnd.nextInt(100) == 0) {
            Iterator iter = dbm.makeIterator();
            if (dbm.isOrdered() && rnd.nextInt(3) == 0) {
              if (rnd.nextInt(3) == 0) {
                iter.jump(key);
              } else {
                iter.last();
              }
              while (rnd.nextInt(10) != 0) {
                Status status = new Status();
                iter.getString(status);
                if (!status.equals(Status.SUCCESS)) {
                  if (!status.equals(Status.NOT_FOUND_ERROR)) {
                    status.orDie();
                  }
                  break;
                }
                iter.previous();
              }
            } else {
              if (rnd.nextInt(3) == 0) {
                iter.jump(key);
              } else {
                iter.first();
              }
              while (rnd.nextInt(10) != 0) {
                Status status = new Status();
                iter.getString(status);
                if (!status.equals(Status.SUCCESS)) {
                  if (!status.equals(Status.NOT_FOUND_ERROR)) {
                    status.orDie();
                  }
                  break;
                }
                iter.next();
              }
            }
            iter.destruct();
          } else if (rnd.nextInt(3) == 0) {
            Status status = new Status();
            dbm.get(key, status);
            if (!status.equals(Status.NOT_FOUND_ERROR)) {
              status.orDie();
            }
          } else if (rnd.nextInt(3) == 0) {
            Status status = dbm.remove(key);
            if (!status.equals(Status.NOT_FOUND_ERROR)) {
              status.orDie();
            }
          } else if (rnd.nextInt(3) == 0) {
            Status status = dbm.set(key, value, false);
            if (!status.equals(Status.DUPLICATION_ERROR)) {
              status.orDie();
            }
          } else {
            dbm.set(key, value).orDie();
          }
          int seq = i + 1;
          if (thid_ == 0 && seq % (num_iterations / 500) == 0) {
            STDOUT.printf(".");
            if (seq % (num_iterations / 10) == 0) {
              STDOUT.printf(" (%08d)\n", seq);
            }
            STDOUT.flush();
          }
        }
      }
      private int thid_;
    }
    STDOUT.printf("Doing:\n");
    double start_time = getTime();
    Task[] tasks = new Task[num_threads];
    for (int thid = 0; thid < num_threads; thid++) {
      tasks[thid] = new Task(thid);
      tasks[thid].start();
    }
    for (int thid = 0; thid < num_threads; thid++) {
      try {
        tasks[thid].join();
      } catch (java.lang.InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    dbm.synchronize(false).orDie();
    double end_time = getTime();
    double elapsed = end_time - start_time;
    System.gc();
    long mem_usage = Utility.getMemoryUsage() - start_mem_usage;
    STDOUT.printf("Done: num_records=%d file_size=%d time=%.3f qps=%.0f mem=%d\n",
                  dbm.count(), dbm.getFileSize(), elapsed,
                  num_iterations * num_threads / elapsed, mem_usage);
    STDOUT.print("\n");
    dbm.close().orDie();
    dbm.destruct();
    return 0;
  }

  /** The standard output stream. */
  private static final PrintStream STDOUT = System.out;
  /** The standard error stream. */
  private static final PrintStream STDERR = System.err;
  /** The rondom generator. */
  private static final Random RND = new Random();
}

// END OF FILE
