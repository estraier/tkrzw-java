/*************************************************************************************************
 * Java binding of Tkrzw
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

#include <string>
#include <string_view>
#include <map>
#include <memory>
#include <vector>

#include <cstddef>
#include <cstdint>

#include "jni.h"

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_file.h"
#include "tkrzw_file_poly.h"
#include "tkrzw_file_util.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

#include "tkrzw_AsyncDBM.h"
#include "tkrzw_DBM.h"
#include "tkrzw_File.h"
#include "tkrzw_Future.h"
#include "tkrzw_Iterator.h"
#include "tkrzw_Utility.h"

// Global variables.
jstring obj_str_empty;
jclass cls_outofmemory;
jclass cls_nullpointer;
jclass cls_illegalargument;
jclass cls_str;
jclass cls_byteary;
jclass cls_strary;
jclass cls_map;
jmethodID id_map_size;
jmethodID id_map_entryset;
jclass cls_set;
jmethodID id_set_iterator;
jclass cls_iter;
jmethodID id_iter_hasnext;
jmethodID id_iter_next;
jclass cls_mapentry;
jmethodID id_mapentry_getkey;
jmethodID id_mapentry_getvalue;
jclass cls_hashmap;
jmethodID id_hashmap_init;
jmethodID id_hashmap_put;
jclass cls_long;
jmethodID id_long_init;
jclass cls_status;
jmethodID id_status_init;
jobject obj_status_codes[tkrzw::Status::APPLICATION_ERROR+1];
jmethodID id_status_set;
jclass cls_status_code;
jclass cls_status_and;
jmethodID id_status_and_init;
jfieldID id_status_and_status;
jfieldID id_status_and_value;
jclass cls_recproc;
jobject obj_recproc_remove;
jmethodID id_recproc_process;
jclass cls_future;
jmethodID id_future_init;
jfieldID id_future_ptr;
jfieldID id_future_is_str;
jclass cls_statusex;
jmethodID id_statusex_init;
jclass cls_dbm;
jfieldID id_dbm_ptr;
jclass cls_dbmiter;
jfieldID id_dbmiter_ptr;
jmethodID id_dbmiter_init;
jclass cls_asyncdbm;
jfieldID id_asyncdbm_ptr;
jclass cls_file;
jfieldID id_file_ptr;
jobject obj_dbm_any_bytes;

// Makes the global class reference.
jclass MakeClassRef(JNIEnv* env, const char* class_name) {
  return (jclass)env->NewGlobalRef(env->FindClass(class_name));
}

// Initializes global variables.
jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  constexpr jint jni_version = JNI_VERSION_1_8;
  JNIEnv* env = nullptr;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jni_version) != JNI_OK) {
    return JNI_ERR;
  }
  obj_str_empty = (jstring)env->NewGlobalRef(env->NewStringUTF(""));
  cls_outofmemory = MakeClassRef(env, "java/lang/OutOfMemoryError");
  cls_nullpointer = MakeClassRef(env, "java/lang/NullPointerException");
  cls_illegalargument = MakeClassRef(env, "java/lang/IllegalArgumentException");
  cls_str = MakeClassRef(env, "java/lang/String");
  cls_byteary = MakeClassRef(env, "[B");
  cls_map = MakeClassRef(env, "java/util/Map");
  id_map_size = env->GetMethodID(cls_map, "size", "()I");
  id_map_entryset = env->GetMethodID(cls_map, "entrySet", "()Ljava/util/Set;");
  cls_set = MakeClassRef(env, "java/util/Set");
  id_set_iterator = env->GetMethodID(cls_set, "iterator", "()Ljava/util/Iterator;");
  cls_iter = MakeClassRef(env, "java/util/Iterator");
  id_iter_hasnext = env->GetMethodID(cls_iter, "hasNext", "()Z");
  id_iter_next = env->GetMethodID(cls_iter, "next", "()Ljava/lang/Object;");
  cls_mapentry = MakeClassRef(env, "java/util/Map$Entry");
  id_mapentry_getkey = env->GetMethodID(cls_mapentry, "getKey", "()Ljava/lang/Object;");
  id_mapentry_getvalue = env->GetMethodID(cls_mapentry, "getValue", "()Ljava/lang/Object;");
  cls_hashmap = MakeClassRef(env, "java/util/HashMap");
  id_hashmap_init = env->GetMethodID(cls_hashmap, "<init>", "(I)V");
  id_hashmap_put = env->GetMethodID(
      cls_hashmap, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
  cls_long = MakeClassRef(env, "java/lang/Long");
  id_long_init = env->GetMethodID(cls_long, "<init>", "(J)V");
  cls_status = MakeClassRef(env, "tkrzw/Status");
  id_status_init = env->GetMethodID(
      cls_status, "<init>", "(Ltkrzw/Status$Code;Ljava/lang/String;)V");
  cls_status_code = MakeClassRef(env, "tkrzw/Status$Code");
  for (int32_t code = 0; code <= tkrzw::Status::APPLICATION_ERROR; code++) {
    const char* const code_name = tkrzw::Status::CodeName((tkrzw::Status::Code)code);
    jfieldID id_code = env->GetStaticFieldID(cls_status_code, code_name, "Ltkrzw/Status$Code;");
    jobject jcode = env->GetStaticObjectField(cls_status_code, id_code);
    obj_status_codes[code] = env->NewGlobalRef(jcode);
  }
  id_status_set =
      env->GetMethodID(cls_status, "set", "(Ltkrzw/Status$Code;Ljava/lang/String;)V");
  cls_status_and = MakeClassRef(env, "tkrzw/Status$And");
  id_status_and_init = env->GetMethodID(cls_status_and, "<init>", "()V");
  id_status_and_status = env->GetFieldID(cls_status_and, "status", "Ltkrzw/Status;");
  id_status_and_value = env->GetFieldID(cls_status_and, "value", "Ljava/lang/Object;");
  cls_recproc = MakeClassRef(env, "tkrzw/RecordProcessor");
  const jfieldID id_obj_recproc_remove = env->GetStaticFieldID(cls_recproc, "REMOVE", "[B");
  obj_recproc_remove = env->NewGlobalRef(env->NewByteArray(0));
  env->SetStaticObjectField(cls_recproc, id_obj_recproc_remove, obj_recproc_remove);
  id_recproc_process = env->GetMethodID(cls_recproc, "process", "([B[B)[B");
  cls_future = MakeClassRef(env, "tkrzw/Future");
  id_future_init = env->GetMethodID(cls_future, "<init>", "()V");
  id_future_ptr = env->GetFieldID(cls_future, "ptr_", "J");
  id_future_is_str = env->GetFieldID(cls_future, "is_str_", "Z");
  cls_statusex = MakeClassRef(env, "tkrzw/StatusException");
  id_statusex_init = env->GetMethodID(cls_statusex, "<init>", "(Ltkrzw/Status;)V");
  cls_dbm = MakeClassRef(env, "tkrzw/DBM");
  id_dbm_ptr = env->GetFieldID(cls_dbm, "ptr_", "J");
  cls_dbmiter = MakeClassRef(env, "tkrzw/Iterator");
  id_dbmiter_ptr = env->GetFieldID(cls_dbmiter, "ptr_", "J");
  id_dbmiter_init = env->GetMethodID(cls_dbmiter, "<init>", "(Ltkrzw/DBM;)V");
  cls_asyncdbm = MakeClassRef(env, "tkrzw/AsyncDBM");
  id_asyncdbm_ptr = env->GetFieldID(cls_asyncdbm, "ptr_", "J");
  cls_file = MakeClassRef(env, "tkrzw/File");
  id_file_ptr = env->GetFieldID(cls_file, "ptr_", "J");
  const jfieldID id_obj_dbm_any_bytes = env->GetStaticFieldID(cls_dbm, "ANY_BYTES", "[B");
  obj_dbm_any_bytes = env->NewGlobalRef(env->NewByteArray(0));
  env->SetStaticObjectField(cls_dbm, id_obj_dbm_any_bytes, obj_dbm_any_bytes);
  return jni_version;
}

// Throws the out-of-memory error.
static void ThrowOutOfMemory(JNIEnv* env) {
  env->ThrowNew(cls_outofmemory, "out of memory");
}

// Throws the null pointer exception.
static void ThrowNullPointer(JNIEnv* env) {
  env->ThrowNew(cls_nullpointer, "null object");
}

// Throws the illegal argument exception.
static void ThrowIllegalArgument(JNIEnv* env, const char* msg) {
  env->ThrowNew(cls_illegalargument, msg);
}

// Creates a new string.
static jstring NewString(JNIEnv* env, const char* str) {
  jstring jstr = env->NewStringUTF(str);
  if (!jstr) {
    ThrowOutOfMemory(env);
    throw std::bad_alloc();
  }
  return jstr;
}

// Creates a new byte array.
static jbyteArray NewByteArray(JNIEnv* env, std::string_view str) {
  jbyteArray jbuf = env->NewByteArray(str.size());
  if (!jbuf) {
    ThrowOutOfMemory(env);
    throw std::bad_alloc();
  }
  env->SetByteArrayRegion(jbuf, 0, str.size(), (jbyte*)str.data());
  return jbuf;
}

// Creates a new status object.
static jobject NewStatus(JNIEnv* env, const tkrzw::Status& status) {
  jobject jcode = obj_status_codes[(int)status.GetCode()];
  jstring jmessage = status.HasMessage() ?
      NewString(env, status.GetMessage().c_str()) : obj_str_empty;
  jobject jstatus = env->NewObject(cls_status, id_status_init, jcode, jmessage);
  return jstatus;
}

// Sets the content a status object.
static void SetStatus(JNIEnv* env, const tkrzw::Status& status, jobject jstatus) {
  jobject jcode = obj_status_codes[(int)status.GetCode()];
  jstring jmessage = status.HasMessage() ?
      NewString(env, status.GetMessage().c_str()) : obj_str_empty;
  env->CallVoidMethod(jstatus, id_status_set, jcode, jmessage);
}

// Throws the status exception.
static void ThrowStatus(JNIEnv* env, const tkrzw::Status& status) {
  jobject jstatus = NewStatus(env, status);
  jthrowable jstatusex = (jthrowable)env->NewObject(cls_statusex, id_statusex_init, jstatus);
  env->Throw(jstatusex);
}

// Creates a new future object.
inline jobject NewFuture(JNIEnv* env, tkrzw::StatusFuture* future, bool is_str) {
  jobject jfuture = env->NewObject(cls_future, id_future_init);
  env->SetLongField(jfuture, id_future_ptr, (intptr_t)future);
  env->SetBooleanField(jfuture, id_future_is_str, is_str);
  return jfuture;
}

// Wrapper to treat a Java string as a C++ string.
class SoftString {
 public:
  SoftString(JNIEnv* env, jstring jstr) :
      env_(env), jstr_(jstr), str_(nullptr), copied_(false) {
    if (jstr) {
      str_ = env_->GetStringUTFChars(jstr_, &copied_);
      if (!str_) {
        ThrowOutOfMemory(env);
        throw std::bad_alloc();
      }
    } else {
      str_ = nullptr;
    }
  }

  ~SoftString() {
    if (copied_) env_->ReleaseStringUTFChars(jstr_, str_);
  }

  const char* Get() {
    return str_;
  }

 private:
  JNIEnv* env_;
  jstring jstr_;
  const char* str_;
  jboolean copied_;
};

// Wrapper to treat a Java byte array as a C++ byte array.
class SoftByteArray {
 public:
  SoftByteArray(JNIEnv* env, jbyteArray jary) :
      env_(env), jary_(jary), ary_(nullptr), size_(0), copied_(false) {
    if (jary) {
      ary_ = env_->GetByteArrayElements(jary, &copied_);
      if (!ary_) {
        ThrowOutOfMemory(env);
        throw std::bad_alloc();
      }
      size_ = env_->GetArrayLength(jary);
    } else {
      ary_ = nullptr;
    }
  }

  ~SoftByteArray() {
    if (copied_) env_->ReleaseByteArrayElements(jary_, ary_, JNI_ABORT);
  }

  std::string_view Get() {
    return std::string_view((const char*)ary_, size_);
  }

  size_t size() {
    return size_;
  }

 private:
  JNIEnv* env_;
  jbyteArray jary_;
  jbyte* ary_;
  size_t size_;
  jboolean copied_;
};

// Gets the future pointer of the Java future object.
static tkrzw::StatusFuture* GetFuture(JNIEnv* env, jobject jfuture) {
  return (tkrzw::StatusFuture*)(intptr_t)env->GetLongField(jfuture, id_future_ptr);
}

// Sets the future pointer of the Java future object.
static void SetFuture(JNIEnv* env, jobject jfuture, tkrzw::StatusFuture* future) {
  env->SetLongField(jfuture, id_future_ptr, (intptr_t)future);
}

// Gets the DBM pointer of the Java DBM object.
static tkrzw::ParamDBM* GetDBM(JNIEnv* env, jobject jdbm) {
  return (tkrzw::ParamDBM*)(intptr_t)env->GetLongField(jdbm, id_dbm_ptr);
}

// Sets the DBM pointer of the Java DBM object.
static void SetDBM(JNIEnv* env, jobject jdbm, tkrzw::ParamDBM* dbm) {
  env->SetLongField(jdbm, id_dbm_ptr, (intptr_t)dbm);
}

// Gets the iterator pointer of the Java iterator object.
static tkrzw::DBM::Iterator* GetIter(JNIEnv* env, jobject jiter) {
  return (tkrzw::DBM::Iterator*)(intptr_t)env->GetLongField(jiter, id_dbmiter_ptr);
}

// Sets the iterator pointer of the Java iterator object.
static void SetIter(JNIEnv* env, jobject jiter, tkrzw::DBM::Iterator* iter) {
  env->SetLongField(jiter, id_dbmiter_ptr, (intptr_t)iter);
}

// Gets the AsyncDBM pointer of the Java AsyncDBM object.
static tkrzw::AsyncDBM* GetAsyncDBM(JNIEnv* env, jobject jasyncdbm) {
  return (tkrzw::AsyncDBM*)(intptr_t)env->GetLongField(jasyncdbm, id_asyncdbm_ptr);
}

// Sets the AsyncDBM pointer of the Java AsyncDBM object.
static void SetAsyncDBM(JNIEnv* env, jobject jasyncdbm, tkrzw::AsyncDBM* asyncdbm) {
  env->SetLongField(jasyncdbm, id_asyncdbm_ptr, (intptr_t)asyncdbm);
}

// Gets the file pointer of the Java text file object.
static tkrzw::PolyFile* GetFile(JNIEnv* env, jobject jfile) {
  return (tkrzw::PolyFile*)(intptr_t)env->GetLongField(jfile, id_file_ptr);
}

// Sets the file pointer of the Java text file object.
static void SetFile(JNIEnv* env, jobject jfile, tkrzw::PolyFile* file) {
  env->SetLongField(jfile, id_file_ptr, (intptr_t)file);
}

// Converts a Java byte array map into a C++ string map.
static std::map<std::string, std::string> JMapToCMap(JNIEnv* env, jobject jmap) {
  std::map<std::string, std::string> map;
  jobject jentryset = env->CallObjectMethod(jmap, id_map_entryset);
  jobject jentrysetiter = env->CallObjectMethod(jentryset, id_set_iterator);
  while (env->CallBooleanMethod(jentrysetiter, id_iter_hasnext)) {
    jobject jmapentry = env->CallObjectMethod(jentrysetiter, id_iter_next);
    jbyteArray jkey = (jbyteArray)env->CallObjectMethod(jmapentry, id_mapentry_getkey);
    jbyteArray jvalue = (jbyteArray)env->CallObjectMethod(jmapentry, id_mapentry_getvalue);
    {
      SoftByteArray key(env, jkey);
      SoftByteArray value(env, jvalue);
      map.emplace(key.Get(), value.Get());
    }
  }
  return map;
}

// Converts a Java string map into a C++ string map.
static std::map<std::string, std::string> JMapToCMapStr(JNIEnv* env, jobject jmap) {
  std::map<std::string, std::string> map;
  jobject jentryset = env->CallObjectMethod(jmap, id_map_entryset);
  jobject jentrysetiter = env->CallObjectMethod(jentryset, id_set_iterator);
  while (env->CallBooleanMethod(jentrysetiter, id_iter_hasnext)) {
    jobject jmapentry = env->CallObjectMethod(jentrysetiter, id_iter_next);
    jstring jkey = (jstring)env->CallObjectMethod(jmapentry, id_mapentry_getkey);
    jstring jvalue = (jstring)env->CallObjectMethod(jmapentry, id_mapentry_getvalue);
    {
      SoftString key(env, jkey);
      SoftString value(env, jvalue);
      map.emplace(key.Get(), value.Get());
    }
  }
  return map;
}

// Converts a C++ string map into a Java byte array map.
static jobject CMapToJMap(JNIEnv* env, const std::map<std::string, std::string>& map) {
  jobject jmap = env->NewObject(cls_hashmap, id_hashmap_init, map.size() * 2 + 1);
  for (const auto& rec : map) {
    jbyteArray jkey = NewByteArray(env, rec.first);
    jbyteArray jvalue = NewByteArray(env, rec.second);
    env->CallObjectMethod(jmap, id_hashmap_put, jkey, jvalue);
  }
  return jmap;
}

// Converts a Java string map into a C++ string map.
static std::map<std::string, std::string> JMapStrToCMap(JNIEnv* env, jobject jmap) {
  std::map<std::string, std::string> map;
  jobject jentryset = env->CallObjectMethod(jmap, id_map_entryset);
  jobject jentrysetiter = env->CallObjectMethod(jentryset, id_set_iterator);
  while (env->CallBooleanMethod(jentrysetiter, id_iter_hasnext)) {
    jobject jmapentry = env->CallObjectMethod(jentrysetiter, id_iter_next);
    jstring jkey = (jstring)env->CallObjectMethod(jmapentry, id_mapentry_getkey);
    jstring jvalue = (jstring)env->CallObjectMethod(jmapentry, id_mapentry_getvalue);
    {
      SoftString key(env, jkey);
      SoftString value(env, jvalue);
      map.emplace(key.Get(), value.Get());
    }
  }
  return map;
}

// Converts a C++ string map into a Java string map.
static jobject CMapToJMapStr(JNIEnv* env, const std::map<std::string, std::string>& map) {
  jobject jmap = env->NewObject(cls_hashmap, id_hashmap_init, map.size() * 2 + 1);
  for (const auto& rec : map) {
    jstring jkey = NewString(env, rec.first.c_str());
    jstring jvalue = NewString(env, rec.second.c_str());
    env->CallObjectMethod(jmap, id_hashmap_put, jkey, jvalue);
  }
  return jmap;
}

// Extracts a list of pairs of string views from a Java string map.
static std::vector<std::pair<std::string_view, std::string_view>> ExtractSVPairs(
    JNIEnv* env, jobject jmap, std::vector<std::string>* placeholder) {
  std::vector<std::pair<std::string_view, std::string_view>> result;
  jobject jentryset = env->CallObjectMethod(jmap, id_map_entryset);
  jobject jentrysetiter = env->CallObjectMethod(jentryset, id_set_iterator);
  jint map_size = env->CallIntMethod(jmap, id_map_size);
  result.reserve(map_size);
  placeholder->reserve(map_size * 2);
  while (env->CallBooleanMethod(jentrysetiter, id_iter_hasnext)) {
    jobject jmapentry = env->CallObjectMethod(jentrysetiter, id_iter_next);
    jbyteArray jkey = (jbyteArray)env->CallObjectMethod(jmapentry, id_mapentry_getkey);
    jbyteArray jvalue = (jbyteArray)env->CallObjectMethod(jmapentry, id_mapentry_getvalue);
    {
      SoftByteArray key(env, jkey);
      placeholder->emplace_back(std::string(key.Get()));
      std::string_view key_view = placeholder->back();
      std::string_view value_view;
      if (jvalue != nullptr) {
        if (env->IsSameObject(jvalue, obj_dbm_any_bytes)) {
          value_view = tkrzw::DBM::ANY_DATA;
        } else {
          SoftByteArray value(env, jvalue);
          placeholder->emplace_back(std::string(value.Get()));
          value_view = placeholder->back();
        }
      }
      result.emplace_back(std::make_pair(key_view, value_view));
    }
  }
  return result;
}

// Converts a Java string into a UCS4 vector.
static std::vector<uint32_t> JStrToUCS4(JNIEnv* env, jstring jstr) {
  SoftString str(env, jstr);
  return tkrzw::ConvertUTF8ToUCS4(str.Get());
}

// Implementation of Utility.getVersion.
JNIEXPORT jstring JNICALL Java_tkrzw_Utility_getVersion
(JNIEnv* env, jclass jcls) {
  return NewString(env, tkrzw::PACKAGE_VERSION);
}

// Implementation of Utility.getOSName.
JNIEXPORT jstring JNICALL Java_tkrzw_Utility_getOSName
(JNIEnv* env, jclass jcls) {
  return NewString(env, tkrzw::OS_NAME);
}

JNIEXPORT jint JNICALL Java_tkrzw_Utility_getPageSize
(JNIEnv* env, jclass jcls) {
  return tkrzw::PAGE_SIZE;
}

// Implementation of Utility.getMemoryCapacity.
JNIEXPORT jlong JNICALL Java_tkrzw_Utility_getMemoryCapacity
(JNIEnv* env, jclass jcls) {
  return tkrzw::GetMemoryCapacity();
}

// Implementation of Utility.getMemoryUsage.
JNIEXPORT jlong JNICALL Java_tkrzw_Utility_getMemoryUsage
(JNIEnv* env, jclass jcls) {
  return tkrzw::GetMemoryUsage();
}

// Implementation of Utility#primaryHash.
JNIEXPORT jlong JNICALL Java_tkrzw_Utility_primaryHash
(JNIEnv* env, jclass jcls, jbyteArray jdata, jlong num_buckets) {
  SoftByteArray data(env, jdata);
  const uint64_t mod_num_buckets = num_buckets > 0 ? num_buckets : tkrzw::UINT64MAX;
  return tkrzw::PrimaryHash(data.Get(), mod_num_buckets);
}

// Implementation of Utility#secondaryHash.
JNIEXPORT jlong JNICALL Java_tkrzw_Utility_secondaryHash
(JNIEnv* env, jclass jcls, jbyteArray jdata, jlong num_shards) {
  SoftByteArray data(env, jdata);
  const uint64_t mod_num_shards = num_shards > 0 ? num_shards : tkrzw::UINT64MAX;
  return tkrzw::SecondaryHash(data.Get(), mod_num_shards);
}

// Implementation of Utility#editDistanceLev.
JNIEXPORT jint JNICALL Java_tkrzw_Utility_editDistanceLev
(JNIEnv* env, jclass jcls, jstring jstra, jstring jstrb) {
  const std::vector<uint32_t> ucsa = JStrToUCS4(env, jstra);
  const std::vector<uint32_t> ucsb = JStrToUCS4(env, jstrb);
  return tkrzw::EditDistanceLev<std::vector<uint32_t>>(ucsa, ucsb);
}

// Implementation of Future#destruct.
JNIEXPORT void JNICALL Java_tkrzw_Future_destruct
(JNIEnv* env, jobject jself) {
  tkrzw::StatusFuture* future = GetFuture(env, jself);
  if (future != nullptr) {
    delete future;
    SetFuture(env, jself, nullptr);
  }
}

// Implementation of Future#await.
JNIEXPORT jboolean JNICALL Java_tkrzw_Future_await
(JNIEnv* env, jobject jself, jdouble timeout) {
  tkrzw::StatusFuture* future = GetFuture(env, jself);
  if (future == nullptr) {
    ThrowNullPointer(env);
    return false;
  }
  return future->Wait(timeout);
}

// Implementation of Future#get.
JNIEXPORT jobject JNICALL Java_tkrzw_Future_get
(JNIEnv* env, jobject jself) {
  tkrzw::StatusFuture* future = GetFuture(env, jself);
  const auto& type = future->GetExtraType();
  if (type == typeid(tkrzw::Status)) {
    const tkrzw::Status status = future->Get();
    delete future;
    SetFuture(env, jself, nullptr);
    return NewStatus(env, status);
  }
  if (type == typeid(std::pair<tkrzw::Status, std::string>)) {
    const auto& result = future->GetString();
    delete future;
    SetFuture(env, jself, nullptr);
    jobject jand = env->NewObject(cls_status_and, id_status_and_init);
    jobject jstatus = NewStatus(env, result.first);
    env->SetObjectField(jand, id_status_and_status, jstatus);
    if (env->GetBooleanField(jself, id_future_is_str)) {
      jstring jvalue = NewString(env, result.second.c_str());
      env->SetObjectField(jand, id_status_and_value, jvalue);
    } else {
      jbyteArray jvalue = NewByteArray(env, result.second);
      env->SetObjectField(jand, id_status_and_value, jvalue);
    }
    return jand;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::pair<std::string, std::string>>)) {
    const auto& result = future->GetStringPair();
    delete future;
    SetFuture(env, jself, nullptr);
    jobject jand = env->NewObject(cls_status_and, id_status_and_init);
    jobject jstatus = NewStatus(env, result.first);
    env->SetObjectField(jand, id_status_and_status, jstatus);
    if (env->GetBooleanField(jself, id_future_is_str)) {
      jobjectArray jpair = env->NewObjectArray(2, cls_str, nullptr);
      env->SetObjectArrayElement(jpair, 0, NewString(env, result.second.first.c_str()));
      env->SetObjectArrayElement(jpair, 1, NewString(env, result.second.second.c_str()));
      env->SetObjectField(jand, id_status_and_value, jpair);
    } else {
      jobjectArray jpair = env->NewObjectArray(2, cls_byteary, nullptr);
      env->SetObjectArrayElement(jpair, 0, NewByteArray(env, result.second.first));
      env->SetObjectArrayElement(jpair, 1, NewByteArray(env, result.second.second));
      env->SetObjectField(jand, id_status_and_value, jpair);
    }
    return jand;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::vector<std::string>>)) {
    const auto& result = future->GetStringVector();
    delete future;
    SetFuture(env, jself, nullptr);
    jobject jand = env->NewObject(cls_status_and, id_status_and_init);
    jobject jstatus = NewStatus(env, result.first);
    env->SetObjectField(jand, id_status_and_status, jstatus);
    if (env->GetBooleanField(jself, id_future_is_str)) {
      jobjectArray jvalue = env->NewObjectArray(result.second.size(), cls_str, nullptr);
      for (size_t i = 0; i < result.second.size(); i++) {
        jstring jelem = NewString(env, result.second[i].c_str());
        env->SetObjectArrayElement(jvalue, i, jelem);
      }
      env->SetObjectField(jand, id_status_and_value, jvalue);
    } else {
      jobjectArray jvalue = env->NewObjectArray(result.second.size(), cls_byteary, nullptr);
      for (size_t i = 0; i < result.second.size(); i++) {
        jbyteArray jelem = NewByteArray(env, result.second[i]);
        env->SetObjectArrayElement(jvalue, i, jelem);
      }
      env->SetObjectField(jand, id_status_and_value, jvalue);
    }
    return jand;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::map<std::string, std::string>>)) {
    const auto& result = future->GetStringMap();
    delete future;
    SetFuture(env, jself, nullptr);
    jobject jand = env->NewObject(cls_status_and, id_status_and_init);
    jobject jstatus = NewStatus(env, result.first);
    env->SetObjectField(jand, id_status_and_status, jstatus);
    if (env->GetBooleanField(jself, id_future_is_str)) {
      jobject jvalue = CMapToJMapStr(env, result.second);
      env->SetObjectField(jand, id_status_and_value, jvalue);
    } else {
      jobject jvalue = CMapToJMap(env, result.second);
      env->SetObjectField(jand, id_status_and_value, jvalue);
    }
    return jand;
  }
  if (type == typeid(std::pair<tkrzw::Status, int64_t>)) {
    const auto& result = future->GetInteger();
    delete future;
    SetFuture(env, jself, nullptr);
    jobject jand = env->NewObject(cls_status_and, id_status_and_init);
    jobject jstatus = NewStatus(env, result.first);
    env->SetObjectField(jand, id_status_and_status, jstatus);
    jobject jvalue = env->NewObject(cls_long, id_long_init, result.second);
    env->SetObjectField(jand, id_status_and_value, jvalue);
    return jand;
  }
  ThrowIllegalArgument(env, "unknown future type");
  return nullptr;
}

// Implementation of Future#toString.
JNIEXPORT jstring JNICALL Java_tkrzw_Future_toString
(JNIEnv* env, jobject jself) {
  tkrzw::StatusFuture* future = GetFuture(env, jself);
  std::string expr = "tkrzw.Future(";
  if (future == nullptr) {
    expr += "destroyed";
  } else {
    expr += tkrzw::SPrintF("%p", future);
  }
  expr += ")";
  return NewString(env, expr.c_str());
}

// Implementation of DBM#initialize.
JNIEXPORT void JNICALL Java_tkrzw_DBM_initialize
(JNIEnv* env, jobject jself) {
  SetDBM(env, jself, nullptr);
}

// Implementation of DBM#destruct.
JNIEXPORT void JNICALL Java_tkrzw_DBM_destruct
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm != nullptr) {
    delete dbm;
    SetDBM(env, jself, nullptr);
  }
}

// Implementation of DBM#open.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_open
(JNIEnv* env, jobject jself, jstring jpath, jboolean writable, jobject jparams) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm != nullptr) {
    ThrowIllegalArgument(env, "opened database");
    return nullptr;
  }
  if (jpath == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString path(env, jpath);
  std::map<std::string, std::string> params;
  if (jparams != nullptr) {
    params = JMapStrToCMap(env, jparams);
  }
  const int32_t num_shards = tkrzw::StrToInt(tkrzw::SearchMap(params, "num_shards", "-1"));
  int32_t open_options = 0;
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "truncate", "false"))) {
    open_options |= tkrzw::File::OPEN_TRUNCATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_create", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_CREATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_wait", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_WAIT;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_lock", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_LOCK;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
    open_options |= tkrzw::File::OPEN_SYNC_HARD;
  }
  params.erase("truncate");
  params.erase("no_create");
  params.erase("no_wait");
  params.erase("no_lock");
  params.erase("sync_hard");
  if (num_shards >= 0) {
    dbm = new tkrzw::ShardDBM();
  } else {
    dbm = new tkrzw::PolyDBM();
  }
  const tkrzw::Status status = dbm->OpenAdvanced(path.Get(), writable, open_options, params);
  if (status == tkrzw::Status::SUCCESS) {
    SetDBM(env, jself, dbm);
  } else {
    delete dbm;
  }
  return NewStatus(env, status);
}

// Implementation of DBM#close.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_close
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  const tkrzw::Status status = dbm->Close();
  delete dbm;
  SetDBM(env, jself, nullptr);
  return NewStatus(env, status);
}

// Implementation of DBM#process.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_process
(JNIEnv* env, jobject jself, jbyteArray jkey, jobject jproc, jboolean writable) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jproc == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(JNIEnv* env, jobject jproc) : env_(env), jproc_(jproc) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      jbyteArray jkey = NewByteArray(env_, key);
      jbyteArray jvalue = NewByteArray(env_, value);
      jbyteArray jrv =
          (jbyteArray)env_->CallObjectMethod(jproc_, id_recproc_process, jkey, jvalue);
      if (env_->ExceptionOccurred()) {
        return NOOP;
      }
      if (jrv == nullptr) {
        return NOOP;
      }
      if (env_->IsSameObject(jrv, obj_recproc_remove)) {
        return REMOVE;
      }
      new_value_ = std::make_unique<SoftByteArray>(env_, jrv);
      return new_value_->Get();
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      jbyteArray jkey = NewByteArray(env_, key);
      jbyteArray jrv =
          (jbyteArray)env_->CallObjectMethod(jproc_, id_recproc_process, jkey, nullptr);
      if (env_->ExceptionOccurred()) {
        return NOOP;
      }
      if (jrv == nullptr) {
        return NOOP;
      }
      if (env_->IsSameObject(jrv, obj_recproc_remove)) {
        return REMOVE;
      }
      new_value_ = std::make_unique<SoftByteArray>(env_, jrv);
      return new_value_->Get();
    }
   private:
    JNIEnv* env_;
    jobject jproc_;
    std::unique_ptr<SoftByteArray> new_value_;
  };
  Processor proc(env, jproc);
  tkrzw::Status status = dbm->Process(key.Get(), &proc, writable);
  return NewStatus(env, status);
}

// Implementation of DBM#contains.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_contains
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return false;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return false;
  }
  SoftByteArray key(env, jkey);
  const tkrzw::Status status = dbm->Get(key.Get());
  return status == tkrzw::Status::SUCCESS;
}

// Implementation of DBM#get.
JNIEXPORT jbyteArray JNICALL Java_tkrzw_DBM_get___3BLtkrzw_Status_2
(JNIEnv* env, jobject jself, jbyteArray jkey, jobject jstatus) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  std::string value;
  const tkrzw::Status status = dbm->Get(key.Get(), &value);
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return NewByteArray(env, value);
  }
  return nullptr;
}

// Implementation of DBM#get.
JNIEXPORT jstring JNICALL Java_tkrzw_DBM_get__Ljava_lang_String_2Ltkrzw_Status_2
(JNIEnv* env, jobject jself, jstring jkey, jobject jstatus) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  std::string value;
  const tkrzw::Status status = dbm->Get(key.Get(), &value);
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return NewString(env, value.c_str());
  }
  return nullptr;
}

// Implementation of DBM#getMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_getMulti___3_3B
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
    SoftByteArray key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  std::map<std::string, std::string> records;
  dbm->GetMulti(key_views, &records);
  return CMapToJMap(env, records);
}

// Implementation of DBM#getMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_getMulti___3Ljava_lang_String_2
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);
    SoftString key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  std::map<std::string, std::string> records;
  dbm->GetMulti(key_views, &records);
  return CMapToJMapStr(env, records);
}

// Implementation of DBM#set.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_set___3B_3BZ
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue, jboolean overwrite) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  SoftByteArray value(env, jvalue);
  const tkrzw::Status status = dbm->Set(key.Get(), value.Get(), overwrite);
  return NewStatus(env, status);
}

// Implementation of DBM#set.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_set__Ljava_lang_String_2Ljava_lang_String_2Z
(JNIEnv* env, jobject jself, jstring jkey, jstring jvalue, jboolean overwrite) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  SoftString value(env, jvalue);
  const tkrzw::Status status = dbm->Set(key.Get(), value.Get(), overwrite);
  return NewStatus(env, status);
}

// Implementation of DBM#SetMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_setMulti
(JNIEnv* env, jobject jself, jobject jrecords, jboolean overwrite) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jrecords == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMap(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  const tkrzw::Status status = dbm->SetMulti(record_views, overwrite);
  return NewStatus(env, status);
}

// Implementation of DBM#SetMultiString.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_setMultiString
(JNIEnv* env, jobject jself, jobject jrecords, jboolean overwrite) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jrecords == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMapStr(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  const tkrzw::Status status = dbm->SetMulti(record_views, overwrite);
  return NewStatus(env, status);
}

// Implementation of DBM#setAndGet.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_setAndGet
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue, jboolean overwrite) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  SoftByteArray value(env, jvalue);
  tkrzw::Status impl_status(tkrzw::Status::SUCCESS);
  std::string old_value;
  bool hit = false;
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(tkrzw::Status* status, std::string_view value, bool overwrite,
              std::string* old_value, bool* hit)
        : status_(status), value_(value), overwrite_(overwrite),
          old_value_(old_value), hit_(hit) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *old_value_ = value;
      *hit_ = true;
      if (overwrite_) {
        return value_;
      }
      status_->Set(tkrzw::Status::DUPLICATION_ERROR);
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }
   private:
    tkrzw::Status* status_;
    std::string_view value_;
    bool overwrite_;
    std::string* old_value_;
    bool* hit_;
  };
  Processor proc(&impl_status, value.Get(), overwrite, &old_value, &hit);
  tkrzw::Status status = dbm->Process(key.Get(), &proc, true);
  status |= impl_status;
  jobject jstatus = NewStatus(env, status);
  jobject jand = env->NewObject(cls_status_and, id_status_and_init);
  env->SetObjectField(jand, id_status_and_status, jstatus);
  if (hit) {
    jbyteArray jold_value = NewByteArray(env, old_value);
    env->SetObjectField(jand, id_status_and_value, jold_value);
  }
  return jand;
}

// Implementation of DBM#remove.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_remove___3B
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  const tkrzw::Status status = dbm->Remove(key.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#remove.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_remove__Ljava_lang_String_2
(JNIEnv* env, jobject jself, jstring jkey) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  const tkrzw::Status status = dbm->Remove(key.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#removeMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_removeMulti___3_3B
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
    SoftByteArray key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  const tkrzw::Status status = dbm->RemoveMulti(key_views);
  return NewStatus(env, status);
}

// Implementation of DBM#removeMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_removeMulti___3Ljava_lang_String_2
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);
    SoftString key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  const tkrzw::Status status = dbm->RemoveMulti(key_views);
  return NewStatus(env, status);
}

// Implementation of DBM#removeAndGet.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_removeAndGet
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  tkrzw::Status impl_status(tkrzw::Status::SUCCESS);
  std::string old_value;
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(tkrzw::Status* status, std::string* old_value)
        : status_(status), old_value_(old_value) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *old_value_ = value;
      return REMOVE;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(tkrzw::Status::NOT_FOUND_ERROR);
      return NOOP;
    }
   private:
    tkrzw::Status* status_;
    std::string* old_value_;
  };
  Processor proc(&impl_status, &old_value);
  tkrzw::Status status = dbm->Process(key.Get(), &proc, true);
  status |= impl_status;
  jobject jstatus = NewStatus(env, status);
  jobject jand = env->NewObject(cls_status_and, id_status_and_init);
  env->SetObjectField(jand, id_status_and_status, jstatus);
  if (status == tkrzw::Status::SUCCESS) {
    jbyteArray jold_value = NewByteArray(env, old_value);
    env->SetObjectField(jand, id_status_and_value, jold_value);
  }
  return jand;
}

// Implementation of DBM#append.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_append___3B_3B_3B
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue, jbyteArray jdelim) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  SoftByteArray value(env, jvalue);
  SoftByteArray delim(env, jdelim);
  const tkrzw::Status status = dbm->Append(key.Get(), value.Get(), delim.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#append.
JNIEXPORT jobject JNICALL
Java_tkrzw_DBM_append__Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2
(JNIEnv* env, jobject jself, jstring jkey, jstring jvalue, jstring jdelim) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  SoftString value(env, jvalue);
  SoftString delim(env, jdelim);
  const tkrzw::Status status = dbm->Append(key.Get(), value.Get(), delim.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#appendMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_appendMulti__Ljava_util_Map_2_3B
(JNIEnv* env, jobject jself, jobject jrecords, jbyteArray jdelim) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jrecords == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMap(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  SoftByteArray delim(env, jdelim);
  const tkrzw::Status status = dbm->AppendMulti(record_views, delim.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#appendMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_appendMulti__Ljava_util_Map_2Ljava_lang_String_2
(JNIEnv* env, jobject jself, jobject jrecords, jstring jdelim) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jrecords == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMapStr(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  SoftString delim(env, jdelim);
  const tkrzw::Status status = dbm->AppendMulti(record_views, delim.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#compareExchange.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_compareExchange
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jexpected, jbyteArray jdesired) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  std::unique_ptr<SoftByteArray> expected;
  std::string_view expected_view;
  if (jexpected != nullptr) {
    if (env->IsSameObject(jexpected, obj_dbm_any_bytes)) {
      expected_view = tkrzw::DBM::ANY_DATA;
    } else {
      expected = std::make_unique<SoftByteArray>(env, jexpected);
      expected_view = expected->Get();
    }
  }
  std::unique_ptr<SoftByteArray> desired;
  std::string_view desired_view;
  if (jdesired != nullptr) {
    if (env->IsSameObject(jdesired, obj_dbm_any_bytes)) {
      desired_view = tkrzw::DBM::ANY_DATA;
    } else {
      desired = std::make_unique<SoftByteArray>(env, jdesired);
      desired_view = desired->Get();
    }
  }
  const tkrzw::Status status = dbm->CompareExchange(key.Get(), expected_view, desired_view);
  return NewStatus(env, status);
}

// Implementation of DBM#compareExchangeAndGet.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_compareExchangeAndGet
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jexpected, jbyteArray jdesired) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  std::unique_ptr<SoftByteArray> expected;
  std::string_view expected_view;
  if (jexpected != nullptr) {
    if (env->IsSameObject(jexpected, obj_dbm_any_bytes)) {
      expected_view = tkrzw::DBM::ANY_DATA;
    } else {
      expected = std::make_unique<SoftByteArray>(env, jexpected);
      expected_view = expected->Get();
    }
  }
  std::unique_ptr<SoftByteArray> desired;
  std::string_view desired_view;
  if (jdesired != nullptr) {
    if (env->IsSameObject(jdesired, obj_dbm_any_bytes)) {
      desired_view = tkrzw::DBM::ANY_DATA;
    } else {
      desired = std::make_unique<SoftByteArray>(env, jdesired);
      desired_view = desired->Get();
    }
  }
  std::string actual;
  bool found = false;
  const tkrzw::Status status = dbm->CompareExchange(
      key.Get(), expected_view, desired_view, &actual, &found);
  jobject jstatus = NewStatus(env, status);
  jbyteArray jactual = found ? NewByteArray(env, actual) : nullptr;
  jobject jand = env->NewObject(cls_status_and, id_status_and_init);
  env->SetObjectField(jand, id_status_and_status, jstatus);
  env->SetObjectField(jand, id_status_and_value, jactual);
  return jand;
}

// Implementation of DBM#increment.
JNIEXPORT jlong JNICALL Java_tkrzw_DBM_increment
(JNIEnv* env, jobject jself, jbyteArray jkey, jlong inc, jlong init, jobject jstatus) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return -1;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return -1;
  }
  SoftByteArray key(env, jkey);
  int64_t current = 0;
  const tkrzw::Status status = dbm->Increment(key.Get(), inc, &current, init);
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return current;
  }
  return tkrzw::INT64MIN;
}

// Implementation of DBM#compareExchangeMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_compareExchangeMulti
(JNIEnv* env, jobject jself, jobject jexpected, jobject jdesired) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jexpected == nullptr || jdesired == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> expected_ph;
  const auto& expected = ExtractSVPairs(env, jexpected, &expected_ph);
  std::vector<std::string> desired_ph;
  const auto& desired = ExtractSVPairs(env, jdesired, &desired_ph);
  const tkrzw::Status status = dbm->CompareExchangeMulti(expected, desired);
  return NewStatus(env, status);
}

// Implementation of DBM#rekey.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_rekey
(JNIEnv* env, jobject jself, jbyteArray jold_key, jbyteArray jnew_key,
 jboolean overwrite, jboolean copying) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jold_key == nullptr || jnew_key == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray old_key(env, jold_key);
  SoftByteArray new_key(env, jnew_key);
  const tkrzw::Status status = dbm->Rekey(old_key.Get(), new_key.Get(), overwrite, copying);
  return NewStatus(env, status);
}

// Implementation of DBM#popFirst.
JNIEXPORT jobjectArray JNICALL Java_tkrzw_DBM_popFirst
(JNIEnv* env, jobject jself, jobject jstatus) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  std::string key, value;
  const tkrzw::Status status = dbm->PopFirst(&key, &value);
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status == tkrzw::Status::SUCCESS) {
    jobjectArray jrec = env->NewObjectArray(2, cls_byteary, nullptr);
    jbyteArray jkey = NewByteArray(env, key);
    jbyteArray jvalue = NewByteArray(env, value);
    env->SetObjectArrayElement(jrec, 0, jkey);
    env->SetObjectArrayElement(jrec, 1, jvalue);
    return jrec;
  }
  return nullptr;
}

// Implementation of DBM#pushLast.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_pushLast
(JNIEnv* env, jobject jself, jbyteArray jvalue, jdouble wtime) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray value(env, jvalue);
  const tkrzw::Status status = dbm->PushLast(value.Get(), wtime);
  return NewStatus(env, status);
}

// Implementation of DBM#count.
JNIEXPORT jlong JNICALL Java_tkrzw_DBM_count
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return -1;
  }
  return dbm->CountSimple();
}

// Implementation of DBM#getFileSize.
JNIEXPORT jlong JNICALL Java_tkrzw_DBM_getFileSize
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return -1;
  }
  return dbm->GetFileSizeSimple();
}

// Implementation of DBM#getFilePath.
JNIEXPORT jstring JNICALL Java_tkrzw_DBM_getFilePath
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  std::string path;
  const tkrzw::Status status = dbm->GetFilePath(&path);
  if (status == tkrzw::Status::SUCCESS) {
    return NewString(env, path.c_str());
  }
  return nullptr;
}

// Implementation of DBM#getTimestamp.
JNIEXPORT jdouble JNICALL Java_tkrzw_DBM_getTimestamp
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return tkrzw::DOUBLENAN;
  }
  double timestamp = 0;
  const tkrzw::Status status = dbm->GetTimestamp(&timestamp);
  if (status == tkrzw::Status::SUCCESS) {
    return timestamp;
  }
  return tkrzw::DOUBLENAN;
}

// Implementation of DBM#clear.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_clear
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  const tkrzw::Status status = dbm->Clear();
  return NewStatus(env, status);
}

// Implementation of DBM#build.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_rebuild
(JNIEnv* env, jobject jself, jobject jparams) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  std::map<std::string, std::string> params;
  if (jparams != nullptr) {
    params = JMapStrToCMap(env, jparams);
  }
  const tkrzw::Status status = dbm->RebuildAdvanced(params);
  return NewStatus(env, status);
}

// Implementation of DBM#shouldBeRebuilt.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_shouldBeRebuilt
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return false;
  }
  return dbm->ShouldBeRebuiltSimple();
}

// Implementation of DBM#synchronize.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_synchronize
(JNIEnv* env, jobject jself, jboolean hard, jobject jparams) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  std::map<std::string, std::string> params;
  if (jparams != nullptr) {
    params = JMapStrToCMap(env, jparams);
  }
  const tkrzw::Status status = dbm->SynchronizeAdvanced(hard, nullptr, params);
  return NewStatus(env, status);
}

// Implementation of DBM#copyFile.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_copyFileData
(JNIEnv* env, jobject jself, jstring jdestpath, jboolean sync_hard) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jdestpath == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString destpath(env, jdestpath);
  const tkrzw::Status status = dbm->CopyFileData(destpath.Get(), sync_hard);
  return NewStatus(env, status);
}

// Implementation of DBM#export.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_export
(JNIEnv* env, jobject jself, jobject jdest_dbm) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jdest_dbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  tkrzw::ParamDBM* dest_dbm = GetDBM(env, jdest_dbm);
  if (dest_dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  const tkrzw::Status status = dbm->Export(dest_dbm);
  return NewStatus(env, status);
}

JNIEXPORT jobject JNICALL Java_tkrzw_DBM_exportToFlatRecords
(JNIEnv* env, jobject jself, jobject jdest_file) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  tkrzw::PolyFile* dest_file = GetFile(env, jdest_file);
  if (dest_file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = tkrzw::ExportDBMToFlatRecords(dbm, dest_file);
  return NewStatus(env, status);
}

JNIEXPORT jobject JNICALL Java_tkrzw_DBM_importFromFlatRecords
(JNIEnv* env, jobject jself, jobject jsrc_file) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  tkrzw::PolyFile* src_file = GetFile(env, jsrc_file);
  if (src_file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = tkrzw::ImportDBMFromFlatRecords(dbm, src_file);
  return NewStatus(env, status);
}

// Implementation of DBM#exportKeysAsLines.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_exportKeysAsLines
(JNIEnv* env, jobject jself, jobject jdest_file) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  tkrzw::PolyFile* dest_file = GetFile(env, jdest_file);
  if (dest_file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = tkrzw::ExportDBMKeysAsLines(dbm, dest_file);
  return NewStatus(env, status);
}

// Implementation of DBM#inspect.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_inspect
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  const auto& records = dbm->Inspect();
  const std::map<std::string, std::string> rec_map(records.begin(), records.end());
  return CMapToJMapStr(env, rec_map);
}

// Implementation of DBM#isOpen.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_isOpen
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  return dbm == nullptr ? false : true;
}

// Implementation of DBM#isWritable.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_isWritable
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return false;
  }
  return dbm->IsWritable();
}

// Implementation of DBM#isHealthy.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_isHealthy
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return false;
  }
  return dbm->IsHealthy();
}

// Implementation of DBM#isOrdered.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_isOrdered
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return false;
  }
  return dbm->IsOrdered();
}

// Implementation of DBM#search.
JNIEXPORT jobjectArray JNICALL Java_tkrzw_DBM_search
(JNIEnv* env, jobject jself, jstring jmode, jbyteArray jpattern, jint capacity) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jmode == nullptr || jpattern == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString mode(env, jmode);
  SoftByteArray pattern(env, jpattern);
  std::vector<std::string> keys;
  const tkrzw::Status status = SearchDBMModal(dbm, mode.Get(), pattern.Get(), &keys, capacity);
  if (status != tkrzw::Status::SUCCESS) {
    ThrowStatus(env, status);
    return nullptr;
  }
  jobjectArray jkeys = env->NewObjectArray(keys.size(), cls_byteary, nullptr);
  for (size_t i = 0; i < keys.size(); i++) {
    jbyteArray jkey = NewByteArray(env, keys[i]);
    env->SetObjectArrayElement(jkeys, i, jkey);
  }
  return jkeys;
}

// Implementation of DBM#makeIterator.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_makeIterator
(JNIEnv* env, jobject jself) {
  return env->NewObject(cls_dbmiter, id_dbmiter_init, jself);
}

// Implementation of DBM#toString.
JNIEXPORT jstring JNICALL Java_tkrzw_DBM_toString
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  std::string expr = "tkrzw.DBM(";
  if (dbm == nullptr) {
    expr += "unopened";
  } else {
    std::string class_name;
    for (const auto& rec : dbm->Inspect()) {
      if (rec.first == "class") {
        class_name = rec.second;
      }
    }
    const std::string path = dbm->GetFilePathSimple();
    const int64_t count = dbm->CountSimple();
    expr += tkrzw::StrCat("class=", class_name, ", path=", tkrzw::StrEscapeC(path, true),
                          ", count=", count);
  }
  expr += ")";
  return NewString(env, expr.c_str());
}

// Implementation of DBM#restoreDatabase.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_restoreDatabase
(JNIEnv* env, jclass jcls, jstring jold_file_path, jstring jnew_file_path,
 jstring jclass_name, jlong end_offset) {
  SoftString old_file_path(env, jold_file_path);
  SoftString new_file_path(env, jnew_file_path);
  SoftString class_name(env, jclass_name);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  int32_t num_shards = 0;
  if (tkrzw::ShardDBM::GetNumberOfShards(old_file_path.Get(), &num_shards) ==
      tkrzw::Status::SUCCESS) {
    status = tkrzw::ShardDBM::RestoreDatabase(
        old_file_path.Get(), new_file_path.Get(), class_name.Get(), end_offset);
  } else {
    status = tkrzw::PolyDBM::RestoreDatabase(
        old_file_path.Get(), new_file_path.Get(), class_name.Get(), end_offset);
  }
  return NewStatus(env, status);
}

// Implementation of Iterator#initialize.
JNIEXPORT void JNICALL Java_tkrzw_Iterator_initialize
(JNIEnv* env, jobject jself, jobject jdbm) {
  if (jdbm == nullptr) {
    ThrowNullPointer(env);
    return;
  }
  tkrzw::ParamDBM* dbm = GetDBM(env, jdbm);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return;
  }
  SetIter(env, jself, dbm->MakeIterator().release());
}

// Implementation of Iterator#destruct.
JNIEXPORT void JNICALL Java_tkrzw_Iterator_destruct
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter != nullptr) {
    delete iter;
    SetIter(env, jself, nullptr);
  }
}

// Implementation of Iterator#first.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_first
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = iter->First();
  return NewStatus(env, status);
}

// Implementation of Iterator#last.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_last
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = iter->Last();
  return NewStatus(env, status);
}

// Implementation of Iterator#jump.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_jump
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr || jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  const tkrzw::Status status = iter->Jump(key.Get());
  return NewStatus(env, status);
}

// Implementation of Iterator#jumpLower.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_jumpLower
(JNIEnv* env, jobject jself, jbyteArray jkey, jboolean inclusive) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr || jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  const tkrzw::Status status = iter->JumpLower(key.Get(), inclusive);
  return NewStatus(env, status);
}

// Implementation of Iterator#jumpUpper.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_jumpUpper
(JNIEnv* env, jobject jself, jbyteArray jkey, jboolean inclusive) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr || jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  const tkrzw::Status status = iter->JumpUpper(key.Get(), inclusive);
  return NewStatus(env, status);
}

// Implementation of Iterator#next.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_next
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = iter->Next();
  return NewStatus(env, status);
}

// Implementation of Iterator#previous.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_previous
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = iter->Previous();
  return NewStatus(env, status);
}

// Implementation of Iterator#get.
JNIEXPORT jobjectArray JNICALL Java_tkrzw_Iterator_get
(JNIEnv* env, jobject jself, jobject jstatus) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::string key, value;
  const tkrzw::Status status = iter->Get(&key, &value);
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status == tkrzw::Status::SUCCESS) {
    jobjectArray jrec = env->NewObjectArray(2, cls_byteary, nullptr);
    jbyteArray jkey = NewByteArray(env, key);
    jbyteArray jvalue = NewByteArray(env, value);
    env->SetObjectArrayElement(jrec, 0, jkey);
    env->SetObjectArrayElement(jrec, 1, jvalue);
    return jrec;
  }
  return nullptr;
}

// Implementation of Iterator#getKey.
JNIEXPORT jbyteArray JNICALL Java_tkrzw_Iterator_getKey
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::string key;
  const tkrzw::Status status = iter->Get(&key);
  if (status == tkrzw::Status::SUCCESS) {
    return NewByteArray(env, key);
  }
  return nullptr;
}

// Implementation of Iterator#getKey.
JNIEXPORT jbyteArray JNICALL Java_tkrzw_Iterator_getValue
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::string value;
  const tkrzw::Status status = iter->Get(nullptr, &value);
  if (status == tkrzw::Status::SUCCESS) {
    return NewByteArray(env, value);
  }
  return nullptr;
}

// Implementation of Iterator#set.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_set
(JNIEnv* env, jobject jself, jbyteArray jvalue) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr || jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray value(env, jvalue);
  const tkrzw::Status status = iter->Set(value.Get());
  return NewStatus(env, status);
}

// Implementation of Iterator#remove.
JNIEXPORT jobject JNICALL Java_tkrzw_Iterator_remove
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = iter->Remove();
  return NewStatus(env, status);
}

// Implementation of Iterator#get.
JNIEXPORT jobjectArray JNICALL Java_tkrzw_Iterator_step
(JNIEnv* env, jobject jself, jobject jstatus) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::string key, value;
  const tkrzw::Status status = iter->Step(&key, &value);
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status == tkrzw::Status::SUCCESS) {
    jobjectArray jrec = env->NewObjectArray(2, cls_byteary, nullptr);
    jbyteArray jkey = NewByteArray(env, key);
    jbyteArray jvalue = NewByteArray(env, value);
    env->SetObjectArrayElement(jrec, 0, jkey);
    env->SetObjectArrayElement(jrec, 1, jvalue);
    return jrec;
  }
  return nullptr;
}

// Implementation of Iterator#toString.
JNIEXPORT jstring JNICALL Java_tkrzw_Iterator_toString
(JNIEnv* env, jobject jself) {
  tkrzw::DBM::Iterator* iter = GetIter(env, jself);
  std::string expr = "tkrzw.Iterator(";
  if (iter == nullptr) {
    expr += "destructed";
  } else {
    std::string key;
    const tkrzw::Status status = iter->Get(&key);
    if (status != tkrzw::Status::SUCCESS) {
      key = "(unlocated)";
    }
    expr += tkrzw::StrCat("key=", tkrzw::StrEscapeC(key, true));
  }
  expr += ")";
  return NewString(env, expr.c_str());
}

// Implementation of AsyncDBM#initialize.
JNIEXPORT void JNICALL Java_tkrzw_AsyncDBM_initialize
(JNIEnv* env, jobject jself, jobject jdbm, jint num_worker_threads) {
  if (jdbm == nullptr) {
    ThrowNullPointer(env);
    return;
  }
  tkrzw::ParamDBM* dbm = GetDBM(env, jdbm);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return;
  }
  SetAsyncDBM(env, jself, new tkrzw::AsyncDBM(dbm, num_worker_threads));
}

// Implementation of AsyncDBM#destruct.
JNIEXPORT void JNICALL Java_tkrzw_AsyncDBM_destruct
(JNIEnv* env, jobject jself) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm != nullptr) {
    delete asyncdbm;
    SetAsyncDBM(env, jself, nullptr);
  }
}

// Implementation of AsyncDBM#get.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_get___3B
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Get(key.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#get.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_get__Ljava_lang_String_2
(JNIEnv* env, jobject jself, jstring jkey) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Get(key.Get()));
  return NewFuture(env, future, true);
}

// Implementation of AsyncDBM#getMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_getMulti___3_3B
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
    SoftByteArray key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  auto* future = new tkrzw::StatusFuture(asyncdbm->GetMulti(key_views));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBMgetMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_getMulti___3Ljava_lang_String_2
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);
    SoftString key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  auto* future = new tkrzw::StatusFuture(asyncdbm->GetMulti(key_views));
  return NewFuture(env, future, true);
}

// Implementation of AsyncDBM#set.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_set___3B_3BZ
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue, jboolean overwrite) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  SoftByteArray value(env, jvalue);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Set(key.Get(), value.Get(), overwrite));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#set.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_set__Ljava_lang_String_2Ljava_lang_String_2Z
(JNIEnv* env, jobject jself, jstring jkey, jstring jvalue, jboolean overwrite) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  SoftString value(env, jvalue);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Set(key.Get(), value.Get(), overwrite));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#setMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_setMulti
(JNIEnv* env, jobject jself, jobject jrecords, jboolean overwrite) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jrecords == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMap(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->SetMulti(records, overwrite));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#setMultiString.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_setMultiString
(JNIEnv* env, jobject jself, jobject jrecords, jboolean overwrite) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jrecords == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMapStr(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->SetMulti(records, overwrite));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#remove.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_remove___3B
(JNIEnv* env, jobject jself, jbyteArray jkey) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Remove(key.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#remove.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_remove__Ljava_lang_String_2
(JNIEnv* env, jobject jself, jstring jkey) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Remove(key.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#removeMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_removeMulti___3_3B
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jbyteArray jkey = (jbyteArray)env->GetObjectArrayElement(jkeys, i);
    SoftByteArray key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  auto* future = new tkrzw::StatusFuture(asyncdbm->RemoveMulti(key_views));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#removeMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_removeMulti___3Ljava_lang_String_2
(JNIEnv* env, jobject jself, jobjectArray jkeys) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkeys == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> keys;
  const int32_t num_keys = env->GetArrayLength(jkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);
    SoftString key(env, jkey);
    keys.emplace_back(key.Get());
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  auto* future = new tkrzw::StatusFuture(asyncdbm->RemoveMulti(key_views));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#append.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_append___3B_3B_3B
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jvalue, jbyteArray jdelim) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  SoftByteArray value(env, jvalue);
  SoftByteArray delim(env, jdelim);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Append(key.Get(), value.Get(), delim.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#append.
JNIEXPORT jobject JNICALL
Java_tkrzw_AsyncDBM_append__Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2
(JNIEnv* env, jobject jself, jstring jkey, jstring jvalue, jstring jdelim) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr || jvalue == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString key(env, jkey);
  SoftString value(env, jvalue);
  SoftString delim(env, jdelim);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Append(key.Get(), value.Get(), delim.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#appendMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_appendMulti__Ljava_util_Map_2_3B
(JNIEnv* env, jobject jself, jobject jrecords, jbyteArray jdelim) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jrecords == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMap(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  SoftByteArray delim(env, jdelim);
  auto* future = new tkrzw::StatusFuture(asyncdbm->AppendMulti(record_views, delim.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#appendMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_appendMulti__Ljava_util_Map_2Ljava_lang_String_2
(JNIEnv* env, jobject jself, jobject jrecords, jstring jdelim) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jrecords == nullptr || jdelim == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const std::map<std::string, std::string>& records = JMapToCMapStr(env, jrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  SoftString delim(env, jdelim);
  auto* future = new tkrzw::StatusFuture(asyncdbm->AppendMulti(record_views, delim.Get()));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#compareExchange.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_compareExchange
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jexpected, jbyteArray jdesired) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  std::unique_ptr<SoftByteArray> expected;
  std::string_view expected_view;
  if (jexpected != nullptr) {
    if (env->IsSameObject(jexpected, obj_dbm_any_bytes)) {
      expected_view = tkrzw::DBM::ANY_DATA;
    } else {
      expected = std::make_unique<SoftByteArray>(env, jexpected);
      expected_view = expected->Get();
    }
  }
  std::unique_ptr<SoftByteArray> desired;
  std::string_view desired_view;
  if (jdesired != nullptr) {
    if (env->IsSameObject(jdesired, obj_dbm_any_bytes)) {
      desired_view = tkrzw::DBM::ANY_DATA;
    } else {
      desired = std::make_unique<SoftByteArray>(env, jdesired);
      desired_view = desired->Get();
    }
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->CompareExchange(
      key.Get(), expected_view, desired_view));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#increment.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_increment
(JNIEnv* env, jobject jself, jbyteArray jkey, jlong inc, jlong init) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jkey == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Increment(key.Get(), inc, init));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#compareExchangeMulti.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_compareExchangeMulti
(JNIEnv* env, jobject jself, jobject jexpected, jobject jdesired) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jexpected == nullptr || jdesired == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::vector<std::string> expected_ph;
  const auto& expected = ExtractSVPairs(env, jexpected, &expected_ph);
  std::vector<std::string> desired_ph;
  const auto& desired = ExtractSVPairs(env, jdesired, &desired_ph);
  auto* future = new tkrzw::StatusFuture(asyncdbm->CompareExchangeMulti(expected, desired));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#rekey.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_rekey
(JNIEnv* env, jobject jself, jbyteArray jold_key, jbyteArray jnew_key,
 jboolean overwrite, jboolean copying) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jold_key == nullptr || jnew_key == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray old_key(env, jold_key);
  SoftByteArray new_key(env, jnew_key);
  auto* future = new tkrzw::StatusFuture(asyncdbm->Rekey(
      old_key.Get(), new_key.Get(), overwrite, copying));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#popFirst.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_popFirst
(JNIEnv* env, jobject jself) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->PopFirst());
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#popFirstString.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_popFirstString
(JNIEnv* env, jobject jself) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->PopFirst());
  return NewFuture(env, future, true);
}

// Implementation of AsyncDBM#pushLast.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_pushLast
(JNIEnv* env, jobject jself, jbyteArray jvalue, jdouble wtime) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jvalue == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray value(env, jvalue);
  auto* future = new tkrzw::StatusFuture(asyncdbm->PushLast(value.Get(), wtime));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#clear.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_clear
(JNIEnv* env, jobject jself) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->Clear());
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#rebuild.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_rebuild
(JNIEnv* env, jobject jself, jobject jparams) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::map<std::string, std::string> params;
  if (jparams != nullptr) {
    params = JMapStrToCMap(env, jparams);
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->Rebuild(params));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#synchronize.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_synchronize
(JNIEnv* env, jobject jself, jboolean hard, jobject jparams) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::map<std::string, std::string> params;
  if (jparams != nullptr) {
    params = JMapStrToCMap(env, jparams);
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->Synchronize(hard, nullptr, params));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#copyFileData.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_copyFileData
(JNIEnv* env, jobject jself, jstring jdestpath, jboolean sync_hard) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jdestpath == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString destpath(env, jdestpath);
  auto* future = new tkrzw::StatusFuture(asyncdbm->CopyFileData(destpath.Get(), sync_hard));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#export.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_export
(JNIEnv* env, jobject jself, jobject jdest_dbm) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jdest_dbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  tkrzw::ParamDBM* dest_dbm = GetDBM(env, jdest_dbm);
  if (dest_dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->Export(dest_dbm));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#exportToFlatRecords.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_exportToFlatRecords
(JNIEnv* env, jobject jself, jobject jdest_file) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  tkrzw::PolyFile* dest_file = GetFile(env, jdest_file);
  if (dest_file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->ExportToFlatRecords(dest_file));
  return NewFuture(env, future, false);
}

// Implementation of AsyncDBM#importFromFlatRecords.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_importFromFlatRecords
(JNIEnv* env, jobject jself, jobject jsrc_file) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  tkrzw::PolyFile* src_file = GetFile(env, jsrc_file);
  if (src_file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  auto* future = new tkrzw::StatusFuture(asyncdbm->ImportFromFlatRecords(src_file));
  return NewFuture(env, future, false);
}

// Implementation of DBM#search.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_search__Ljava_lang_String_2_3BI
(JNIEnv* env, jobject jself, jstring jmode, jbyteArray jpattern, jint capacity) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jmode == nullptr || jpattern == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString mode(env, jmode);
  SoftByteArray pattern(env, jpattern);
  auto* future = new tkrzw::StatusFuture(asyncdbm->SearchModal(
      mode.Get(), pattern.Get(), capacity));
  return NewFuture(env, future, false);
}

// Implementation of DBM#search.
JNIEXPORT jobject JNICALL Java_tkrzw_AsyncDBM_search__Ljava_lang_String_2Ljava_lang_String_2I
(JNIEnv* env, jobject jself, jstring jmode, jstring jpattern, jint capacity) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  if (asyncdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jmode == nullptr || jpattern == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString mode(env, jmode);
  SoftString pattern(env, jpattern);
  auto* future = new tkrzw::StatusFuture(asyncdbm->SearchModal(
      mode.Get(), pattern.Get(), capacity));
  return NewFuture(env, future, true);
}

// Implementation of AsyncDBM#toString.
JNIEXPORT jstring JNICALL Java_tkrzw_AsyncDBM_toString
(JNIEnv* env, jobject jself) {
  tkrzw::AsyncDBM* asyncdbm = GetAsyncDBM(env, jself);
  std::string expr = "tkrzw.AsyncDBM(";
  if (asyncdbm == nullptr) {
    expr += "destroyed";
  } else {
    expr += tkrzw::SPrintF("%p", asyncdbm);
  }
  expr += ")";
  return NewString(env, expr.c_str());
}

// Implementation of File#initialize.
JNIEXPORT void JNICALL Java_tkrzw_File_initialize
(JNIEnv* env, jobject jself){
  tkrzw::PolyFile* file = new tkrzw::PolyFile;
  SetFile(env, jself, file);
}

// Implementation of File#destruct.
JNIEXPORT void JNICALL Java_tkrzw_File_destruct
(JNIEnv* env, jobject jself) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file != nullptr) {
    delete file;
    SetFile(env, jself, nullptr);
  }
}

// Implementation of File#open.
JNIEXPORT jobject JNICALL Java_tkrzw_File_open
(JNIEnv* env, jobject jself, jstring jpath, jboolean writable, jobject jparams) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr || jpath == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString path(env, jpath);
  std::map<std::string, std::string> params;
  if (jparams != nullptr) {
    params = JMapStrToCMap(env, jparams);
  }
  int32_t open_options = 0;
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "truncate", "false"))) {
    open_options |= tkrzw::File::OPEN_TRUNCATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_create", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_CREATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_wait", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_WAIT;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_lock", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_LOCK;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
    open_options |= tkrzw::File::OPEN_SYNC_HARD;
  }
  const tkrzw::Status status = file->OpenAdvanced(
      std::string(path.Get()), writable, open_options, params);
  return NewStatus(env, status);
}

// Implementation of File#close.
JNIEXPORT jobject JNICALL Java_tkrzw_File_close
(JNIEnv* env, jobject jself) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = file->Close();
  return NewStatus(env, status);
}


// Implementation of File#read.
JNIEXPORT jobject JNICALL Java_tkrzw_File_read
(JNIEnv* env, jobject jself, jlong off, jbyteArray jbuf, jlong size) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jbuf == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (size < 0 || size > env->GetArrayLength(jbuf)) {
    ThrowIllegalArgument(env, "invalid size");
    return nullptr;
  }
  jboolean copied = false;
  jbyte* buf_ptr = env->GetByteArrayElements(jbuf, &copied);
  if (buf_ptr == nullptr) {
    ThrowOutOfMemory(env);
    return nullptr;
  }
  const tkrzw::Status status = file->Read(off, (char*)buf_ptr, size);
  if (copied) {
    env->ReleaseByteArrayElements(jbuf, buf_ptr, 0);
  }
  return NewStatus(env, status);
}

// Implementation of File#write.
JNIEXPORT jobject JNICALL Java_tkrzw_File_write
(JNIEnv* env, jobject jself, jlong off, jbyteArray jbuf, jlong size) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (jbuf == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  if (size < 0 || size > env->GetArrayLength(jbuf)) {
    ThrowIllegalArgument(env, "invalid size");
    return nullptr;
  }
  jboolean copied = false;
  jbyte* buf_ptr = env->GetByteArrayElements(jbuf, &copied);
  if (buf_ptr == nullptr) {
    ThrowOutOfMemory(env);
    return nullptr;
  }
  const tkrzw::Status status = file->Write(off, (char*)buf_ptr, size);
  if (copied) {
    env->ReleaseByteArrayElements(jbuf, buf_ptr, JNI_ABORT);
  }
  return NewStatus(env, status);
}

// Implementation of File#append.
JNIEXPORT jlong JNICALL Java_tkrzw_File_append
(JNIEnv* env, jobject jself, jbyteArray jbuf, jlong size, jobject jstatus) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return -1;
  }
  if (jbuf == nullptr) {
    ThrowNullPointer(env);
    return -1;
  }
  if (size < 0 || size > env->GetArrayLength(jbuf)) {
    ThrowIllegalArgument(env, "invalid size");
    return -1;
  }
  jboolean copied = false;
  jbyte* buf_ptr = env->GetByteArrayElements(jbuf, &copied);
  if (buf_ptr == nullptr) {
    ThrowOutOfMemory(env);
    return -1;
  }
  int64_t off = 0;
  const tkrzw::Status status = file->Append((char*)buf_ptr, size, &off);
  if (copied) {
    env->ReleaseByteArrayElements(jbuf, buf_ptr, JNI_ABORT);
  }
  if (jstatus != nullptr) {
    SetStatus(env, status, jstatus);
  }
  if (status != tkrzw::Status::SUCCESS) {
    off = -1;
  }
  return off;
}

// Implementation of File#truncate.
JNIEXPORT jobject JNICALL Java_tkrzw_File_truncate
(JNIEnv* env, jobject jself, jlong size) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = file->Truncate(size);
  return NewStatus(env, status);
}

JNIEXPORT jobject JNICALL Java_tkrzw_File_synchronize
(JNIEnv* env, jobject jself, jboolean hard, jlong off, jlong size) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = file->Synchronize(hard, off, size);
  return NewStatus(env, status);
}

// Implementation of File#getSize.
JNIEXPORT jlong JNICALL Java_tkrzw_File_getSize
(JNIEnv* env, jobject jself) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return -1;
  }
  return file->GetSizeSimple();
}

// Implementation of File#getPath.
JNIEXPORT jstring JNICALL Java_tkrzw_File_getPath
(JNIEnv* env, jobject jself) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  std::string path;
  const tkrzw::Status status = file->GetPath(&path);
  if (status == tkrzw::Status::SUCCESS) {
    return NewString(env, path.c_str());
  }
  return nullptr;
}

// Implementation of File#search.
JNIEXPORT jobjectArray JNICALL Java_tkrzw_File_search
(JNIEnv* env, jobject jself, jstring jmode, jbyteArray jpattern, jint capacity) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  if (file == nullptr || jmode == nullptr || jpattern == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString mode(env, jmode);
  SoftByteArray pattern(env, jpattern);
  std::vector<std::string> lines;
  const tkrzw::Status status =
      SearchTextFileModal(file, mode.Get(), pattern.Get(), &lines, capacity);
  if (status != tkrzw::Status::SUCCESS) {
    ThrowStatus(env, status);
    return nullptr;
  }
  jobjectArray jlines = env->NewObjectArray(lines.size(), cls_byteary, nullptr);
  for (size_t i = 0; i < lines.size(); i++) {
    jbyteArray jline = NewByteArray(env, lines[i]);
    env->SetObjectArrayElement(jlines, i, jline);
  }
  return jlines;
}

// Implementation of File#toString.
JNIEXPORT jstring JNICALL Java_tkrzw_File_toString
(JNIEnv* env, jobject jself) {
  tkrzw::PolyFile* file = GetFile(env, jself);
  std::string expr = "tkrzw.File(";
  std::string class_name = "unknown";
  auto* in_file = file->GetInternalFile();
  if (in_file != nullptr) {
    const auto& file_type = in_file->GetType();
    if (file_type == typeid(tkrzw::StdFile)) {
      class_name = "StdFile";
    } else if (file_type == typeid(tkrzw::MemoryMapParallelFile)) {
      class_name = "MemoryMapParallelFile";
    } else if (file_type == typeid(tkrzw::MemoryMapAtomicFile)) {
      class_name = "MemoryMapAtomicFile";
    } else if (file_type == typeid(tkrzw::PositionalParallelFile)) {
      class_name = "PositionalParallelFile";
    } else if (file_type == typeid(tkrzw::PositionalAtomicFile)) {
      class_name = "PositionalAtomicFile";
    }
  }
  const std::string path = file->GetPathSimple();
  const int64_t count = file->GetSizeSimple();
  expr += tkrzw::StrCat("class=", class_name, ", path=", tkrzw::StrEscapeC(path, true),
                        ", size=", count);
  expr += ")";
  return NewString(env, expr.c_str());
}

// END OF FILE
