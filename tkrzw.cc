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
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_util.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

#include "tkrzw_DBM.h"
#include "tkrzw_Iterator.h"
#include "tkrzw_TextFile.h"
#include "tkrzw_Utility.h"

// Throws the out-of-memory error.
static void ThrowOutOfMemory(JNIEnv* env) {
  jclass cls = env->FindClass("java/lang/OutOfMemoryError");
  env->ThrowNew(cls, "out of memory");
}

// Throws the null pointer exception.
static void ThrowNullPointer(JNIEnv* env) {
  jclass cls = env->FindClass("java/lang/NullPointerException");
  env->ThrowNew(cls, "null object");
}

// Throws the illegal argument exception.
static void ThrowIllegalArgument(JNIEnv* env, const char* msg) {
  jclass cls = env->FindClass("java/lang/IllegalArgumentException");
  env->ThrowNew(cls, msg);
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
  const char* const code_name = tkrzw::Status::CodeName(status.GetCode());
  jclass cls_code = env->FindClass("tkrzw/Status$Code");
  jfieldID id_code = env->GetStaticFieldID(cls_code, code_name, "Ltkrzw/Status$Code;");
  jobject jcode = env->GetStaticObjectField(cls_code, id_code);
  jstring jmessage = NewString(env, status.GetMessage().c_str());
  jclass cls_status = env->FindClass("tkrzw/Status");
  jmethodID id_init = env->GetMethodID(
      cls_status, "<init>", "(Ltkrzw/Status$Code;Ljava/lang/String;)V");
  jobject jstatus = env->NewObject(cls_status, id_init, jcode, jmessage);
  env->DeleteLocalRef(jmessage);
  env->DeleteLocalRef(jcode);
  return jstatus;
}

// Sets the content a status object.
static void SetStatus(JNIEnv* env, const tkrzw::Status& status, jobject jstatus) {
  const char* const code_name = tkrzw::Status::CodeName(status.GetCode());
  jclass cls_code = env->FindClass("tkrzw/Status$Code");
  jfieldID id_code = env->GetStaticFieldID(cls_code, code_name, "Ltkrzw/Status$Code;");
  jobject jcode = env->GetStaticObjectField(cls_code, id_code);
  jstring jmessage = NewString(env, status.GetMessage().c_str());
  jclass cls_status = env->GetObjectClass(jstatus);
  jmethodID id_set = env->GetMethodID(
      cls_status, "set", "(Ltkrzw/Status$Code;Ljava/lang/String;)V");
  env->CallVoidMethod(jstatus, id_set, jcode, jmessage);
  env->DeleteLocalRef(jmessage);
  env->DeleteLocalRef(jcode);
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

// Gets the DBM pointer of the Java DBM object.
static tkrzw::ParamDBM* GetDBM(JNIEnv* env, jobject jdbm) {
  jclass cls_dbm = env->GetObjectClass(jdbm);
  jfieldID id_dbm_ptr = env->GetFieldID(cls_dbm, "ptr_", "J");
  return (tkrzw::ParamDBM*)(intptr_t)env->GetLongField(jdbm, id_dbm_ptr);
}

// Sets the DBM pointer of the Java DBM object.
static void SetDBM(JNIEnv* env, jobject jdbm, tkrzw::ParamDBM* dbm) {
  jclass cls_dbm = env->GetObjectClass(jdbm);
  jfieldID id_dbm_ptr = env->GetFieldID(cls_dbm, "ptr_", "J");
  env->SetLongField(jdbm, id_dbm_ptr, (intptr_t)dbm);
}

// Gets the iterator pointer of the Java iterator object.
static tkrzw::DBM::Iterator* GetIter(JNIEnv* env, jobject jiter) {
  jclass cls_iter = env->GetObjectClass(jiter);
  jfieldID id_iter_ptr = env->GetFieldID(cls_iter, "ptr_", "J");
  return (tkrzw::DBM::Iterator*)(intptr_t)env->GetLongField(jiter, id_iter_ptr);
}

// Sets the iterator pointer of the Java iterator object.
static void SetIter(JNIEnv* env, jobject jiter, tkrzw::DBM::Iterator* iter) {
  jclass cls_iter = env->GetObjectClass(jiter);
  jfieldID id_iter_ptr = env->GetFieldID(cls_iter, "ptr_", "J");
  env->SetLongField(jiter, id_iter_ptr, (intptr_t)iter);
}

// Gets the file pointer of the Java text file object.
static tkrzw::File* GetFile(JNIEnv* env, jobject jfile) {
  jclass cls_file = env->GetObjectClass(jfile);
  jfieldID id_file_ptr = env->GetFieldID(cls_file, "ptr_", "J");
  return (tkrzw::File*)(intptr_t)env->GetLongField(jfile, id_file_ptr);
}

// Sets the file pointer of the Java text file object.
static void SetFile(JNIEnv* env, jobject jfile, tkrzw::File* file) {
  jclass cls_file = env->GetObjectClass(jfile);
  jfieldID id_file_ptr = env->GetFieldID(cls_file, "ptr_", "J");
  env->SetLongField(jfile, id_file_ptr, (intptr_t)file);
}

// Converts a Java string map into a C++ string map.
static std::map<std::string, std::string> JMapToCMap(JNIEnv* env, jobject jmap) {
  std::map<std::string, std::string> map;
  jclass cls_map = env->GetObjectClass(jmap);
  jmethodID id_entryset = env->GetMethodID(cls_map, "entrySet", "()Ljava/util/Set;");
  jobject jset = env->CallObjectMethod(jmap, id_entryset);
  jclass cls_set = env->GetObjectClass(jset);
  jmethodID id_iterator = env->GetMethodID(cls_set, "iterator", "()Ljava/util/Iterator;");
  jobject jiter = env->CallObjectMethod(jset, id_iterator);
  jclass cls_iter = env->GetObjectClass(jiter);
  jmethodID id_hasnext = env->GetMethodID(cls_iter, "hasNext", "()Z");
  jmethodID id_next = env->GetMethodID(cls_iter, "next", "()Ljava/lang/Object;");
  while (env->CallBooleanMethod(jiter, id_hasnext)) {
    jobject jpair = env->CallObjectMethod(jiter, id_next);
    jclass cls_pair = env->GetObjectClass(jpair);
    jmethodID id_getkey = env->GetMethodID(cls_pair, "getKey", "()Ljava/lang/Object;");
    jmethodID id_getvalue = env->GetMethodID(cls_pair, "getValue", "()Ljava/lang/Object;");
    jstring jkey = (jstring)env->CallObjectMethod(jpair, id_getkey);
    jstring jvalue = (jstring)env->CallObjectMethod(jpair, id_getvalue);
    SoftString key(env, jkey);
    SoftString value(env, jvalue);
    map.emplace(key.Get(), value.Get());
    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jvalue);
    env->DeleteLocalRef(jpair);
  }
  env->DeleteLocalRef(jiter);
  env->DeleteLocalRef(jset);
  return map;
}

// Converts a C++ string map into a Java string map.
static jobject CMapToJMap(JNIEnv* env, const std::map<std::string, std::string>& map) {
  jclass cls_map = env->FindClass("java/util/HashMap");
  jmethodID id_map_init = env->GetMethodID(cls_map, "<init>", "(I)V");
  jmethodID id_map_put =  env->GetMethodID(
      cls_map, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
  jobject jmap = env->NewObject(cls_map, id_map_init, map.size() * 2 + 1);
  for (const auto& rec : map) {
    jstring jkey = NewString(env, rec.first.c_str());
    jstring jvalue = NewString(env, rec.second.c_str());
    env->CallObjectMethod(jmap, id_map_put, jkey, jvalue);
    env->DeleteLocalRef(jkey);
    env->DeleteLocalRef(jvalue);
  }
  return jmap;
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

// Implementation of Utility.getMemoryUsage.
JNIEXPORT jlong JNICALL Java_tkrzw_Utility_getMemoryUsage
(JNIEnv* env, jclass jcls) {
  const std::map<std::string, std::string> records = tkrzw::GetSystemInfo();
  return tkrzw::StrToInt(tkrzw::SearchMap(records, "mem_rss", "-1"));
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
    params = JMapToCMap(env, jparams);
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
  params.erase("truncate");
  params.erase("no_create");
  params.erase("no_wait");
  params.erase("no_lock");
  if (num_shards >= 0) {
    dbm = new tkrzw::ShardDBM();
  } else {
    dbm = new tkrzw::PolyDBM();
    params.erase("num_shards");
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

// Implementation of DBM#get.
JNIEXPORT jbyteArray JNICALL Java_tkrzw_DBM_get
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

// Implementation of DBM#set.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_set
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

// Implementation of DBM#remove.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_remove
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

// Implementation of DBM#append.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_append
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

// Implementation of DBM#compareExchange.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_compareExchange
(JNIEnv* env, jobject jself, jbyteArray jkey, jbyteArray jexpected, jbyteArray jdesired) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jkey == nullptr || jexpected == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftByteArray key(env, jkey);
  SoftByteArray expected(env, jexpected);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  if (jdesired == nullptr) {
    std::string_view desired;
    status = dbm->CompareExchange(key.Get(), expected.Get(), desired);
  } else {
    SoftByteArray desired(env, jdesired);
    status = dbm->CompareExchange(key.Get(), expected.Get(), desired.Get());
  }
  return NewStatus(env, status);
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
    params = JMapToCMap(env, jparams);
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
    params = JMapToCMap(env, jparams);
  }
  const tkrzw::Status status = dbm->SynchronizeAdvanced(hard, nullptr, params);
  return NewStatus(env, status);
}

// Implementation of DBM#copyFile.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_copyFile
(JNIEnv* env, jobject jself, jstring jdestpath) {
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
  const tkrzw::Status status = dbm->CopyFile(destpath.Get());
  return NewStatus(env, status);
}

// Implementation of DBM#export.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_export
(JNIEnv* env, jobject jself, jobject jdestdbm) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  if (dbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  if (jdestdbm == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  tkrzw::ParamDBM* destdbm = GetDBM(env, jdestdbm);
  if (destdbm == nullptr) {
    ThrowIllegalArgument(env, "not opened database");
    return nullptr;
  }
  const tkrzw::Status status = dbm->Export(destdbm);
  return NewStatus(env, status);
}

// Implementation of DBM#exportKeysAsLines.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_exportKeysAsLines
(JNIEnv* env, jobject jself, jstring jdestpath) {
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
  tkrzw::MemoryMapParallelFile file;
  tkrzw::Status status = file.Open(
      std::string(destpath.Get()), true, tkrzw::File::OPEN_TRUNCATE);
  if (status == tkrzw::Status::SUCCESS) {
    status |= tkrzw::ExportDBMKeysAsLines(dbm, &file);
    status |= file.Close();
  }
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
  return CMapToJMap(env, rec_map);
}

// Implementation of DBM#isOpen.
JNIEXPORT jboolean JNICALL Java_tkrzw_DBM_isOpen
(JNIEnv* env, jobject jself) {
  tkrzw::ParamDBM* dbm = GetDBM(env, jself);
  return dbm == nullptr ? false : true;
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
(JNIEnv* env, jobject jself, jstring jmode, jbyteArray jpattern, jint capacity, jboolean utf) {
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
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  if (std::strcmp(mode.Get(), "contain") == 0) {
    status = tkrzw::SearchDBM(dbm, pattern.Get(), &keys, capacity, tkrzw::StrContains);
  } else if (std::strcmp(mode.Get(), "begin") == 0) {
    status = tkrzw::SearchDBMForwardMatch(dbm, pattern.Get(), &keys, capacity);
  } else if (std::strcmp(mode.Get(), "end") == 0) {
    status = tkrzw::SearchDBM(dbm, pattern.Get(), &keys, capacity, tkrzw::StrEndsWith);
  } else if (std::strcmp(mode.Get(), "regex") == 0) {
    status = tkrzw::SearchDBMRegex(dbm, pattern.Get(), &keys, capacity, utf);
  } else if (std::strcmp(mode.Get(), "edit") == 0) {
    status = tkrzw::SearchDBMEditDistance(dbm, pattern.Get(), &keys, capacity, utf);
  } else {
    ThrowIllegalArgument(env, "unknown mode");
    return nullptr;
  }
  jclass cls_byteary = env->FindClass("[B");
  jobjectArray jkeys = env->NewObjectArray(keys.size(), cls_byteary, nullptr);
  for (size_t i = 0; i < keys.size(); i++) {
    jbyteArray jkey = NewByteArray(env, keys[i]);
    env->SetObjectArrayElement(jkeys, i, jkey);
    env->DeleteLocalRef(jkey);
  }
  return jkeys;
}

// Implementation of DBM#makeIterator.
JNIEXPORT jobject JNICALL Java_tkrzw_DBM_makeIterator
(JNIEnv* env, jobject jself) {
  jclass cls_iter = env->FindClass("tkrzw/Iterator");
  jmethodID id_init = env->GetMethodID(cls_iter, "<init>", "(Ltkrzw/DBM;)V");
  return env->NewObject(cls_iter, id_init, jself);
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
  if (iter == nullptr) {
    ThrowNullPointer(env);
    return;
  }
  delete iter;
  SetIter(env, jself, nullptr);
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
    jclass cls_byteary = env->FindClass("[B");
    jobjectArray jrec = env->NewObjectArray(2, cls_byteary, nullptr);
    jbyteArray jkey = NewByteArray(env, key);
    jbyteArray jvalue = NewByteArray(env, value);
    env->SetObjectArrayElement(jrec, 0, jkey);
    env->SetObjectArrayElement(jrec, 1, jvalue);
    env->DeleteLocalRef(jvalue);
    env->DeleteLocalRef(jkey);
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

// Implementation of TextFile#initialize.
JNIEXPORT void JNICALL Java_tkrzw_TextFile_initialize
(JNIEnv* env, jobject jself){
  tkrzw::File* file = new tkrzw::MemoryMapParallelFile;
  SetFile(env, jself, file);
}

// Implementation of TextFile#destruct.
JNIEXPORT void JNICALL Java_tkrzw_TextFile_destruct
(JNIEnv* env, jobject jself) {
  tkrzw::File* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return;
  }
  delete file;
  SetFile(env, jself, nullptr);
}

// Implementation of TextFile#open.
JNIEXPORT jobject JNICALL Java_tkrzw_TextFile_open
(JNIEnv* env, jobject jself, jstring jpath) {
  tkrzw::File* file = GetFile(env, jself);
  if (file == nullptr || jpath == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString path(env, jpath);
  const tkrzw::Status status = file->Open(std::string(path.Get()), false);
  return NewStatus(env, status);
}

// Implementation of TextFile#close.
JNIEXPORT jobject JNICALL Java_tkrzw_TextFile_close
(JNIEnv* env, jobject jself) {
  tkrzw::File* file = GetFile(env, jself);
  if (file == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  const tkrzw::Status status = file->Close();
  return NewStatus(env, status);
}

// Implementation of TextFile#search.
JNIEXPORT jobjectArray JNICALL Java_tkrzw_TextFile_search
(JNIEnv* env, jobject jself, jstring jmode, jstring jpattern, jint capacity, jboolean utf) {
  tkrzw::File* file = GetFile(env, jself);
  if (file == nullptr || jmode == nullptr || jpattern == nullptr) {
    ThrowNullPointer(env);
    return nullptr;
  }
  SoftString mode(env, jmode);
  SoftString pattern(env, jpattern);
  std::vector<std::string> keys;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  if (std::strcmp(mode.Get(), "contain") == 0) {
    status = tkrzw::SearchTextFile(file, pattern.Get(), &keys, capacity, tkrzw::StrContains);
  } else if (std::strcmp(mode.Get(), "begin") == 0) {
    status = tkrzw::SearchTextFile(file, pattern.Get(), &keys, capacity, tkrzw::StrBeginsWith);
  } else if (std::strcmp(mode.Get(), "end") == 0) {
    status = tkrzw::SearchTextFile(file, pattern.Get(), &keys, capacity, tkrzw::StrEndsWith);
  } else if (std::strcmp(mode.Get(), "regex") == 0) {
    status = tkrzw::SearchTextFileRegex(file, pattern.Get(), &keys, capacity, utf);
  } else if (std::strcmp(mode.Get(), "edit") == 0) {
    status = tkrzw::SearchTextFileEditDistance(file, pattern.Get(), &keys, capacity, utf);
  } else {
    ThrowIllegalArgument(env, "unknown mode");
    return nullptr;
  }
  jclass cls_byteary = env->FindClass("[B");
  jobjectArray jkeys = env->NewObjectArray(keys.size(), cls_byteary, nullptr);
  for (size_t i = 0; i < keys.size(); i++) {
    jbyteArray jkey = NewByteArray(env, keys[i]);
    env->SetObjectArrayElement(jkeys, i, jkey);
    env->DeleteLocalRef(jkey);
  }
  return jkeys;
}

// Implementation of TextFile#toString.
JNIEXPORT jstring JNICALL Java_tkrzw_TextFile_toString
(JNIEnv* env, jobject jself) {
  return NewString(env, "tkrzw.TextFile");
}

// END OF FILE
