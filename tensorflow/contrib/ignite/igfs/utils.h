/* Copyright 2018 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef TF_IGFS_UTILS_H
#define TF_IGFS_UTILS_H

#include <string>
#include <iostream>
#include <map>
#include <memory>
#include <streambuf>
#include <sys/socket.h>

#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/contrib/ignite/kernels/ignite_plain_client.h"

using namespace std;

namespace tensorflow {

class IGFSClient : public PlainClient {
 public:
  IGFSClient(std::string host, int port) : PlainClient(host, port, true) {
    LOG(INFO) << "New IGFS Client " << host << ":" << port;
    pos = 0;
  }

  virtual Status ReadData(uint8_t* buf, int32_t length) {
    TF_RETURN_IF_ERROR(PlainClient::ReadData(buf, length));
    pos += length;

    return Status::OK();
  }

  virtual Status WriteData(uint8_t* buf, int32_t length) {
    TF_RETURN_IF_ERROR(PlainClient::WriteData(buf, length));
    pos += length;

    return Status::OK();
  }

  Status skip(int n) {
    TF_RETURN_IF_ERROR(Ignore(n));

    return Status::OK();
  };

  Status Ignore(int n) {
    uint8_t buf[n];
    return ReadData(buf, n);
  }

  Status skipToPos(int targetPos) {
    int toSkip = std::max(0, targetPos - pos);
    return Ignore(toSkip);
  };

  Status ReadBool(bool& res) {
    uint8_t d = 0;
    TF_RETURN_IF_ERROR(ReadData(&d, 1));
    res = d != 0;

    return Status::OK();
  }

  Status ReadNullableString(string &res) {
    bool isEmpty;
    ReadBool(isEmpty);

    if (isEmpty) {
      res = string();
    } else {
      TF_RETURN_IF_ERROR(ReadString(res));
    }

    return Status::OK();
  }

  Status ReadString(string &res) {
    short len;
    TF_RETURN_IF_ERROR(ReadShort(&len));
    auto *cStr = new uint8_t[len + 1];
    cStr[len] = 0;
    TF_RETURN_IF_ERROR(ReadData(cStr, len));
    res = string((char *)cStr);

    return Status::OK();
  }

  Status ReadStringMap(map<string, string> &res) {
    int size;
    TF_RETURN_IF_ERROR(ReadInt(&size));

    for (int i = 0; i < size; i++) {
      string key;
      string val;
      TF_RETURN_IF_ERROR(ReadString(key));
      TF_RETURN_IF_ERROR(ReadString(val));

      res.insert(std::pair<string, string>(key, val));
    }

    return Status::OK();
  };

  Status writeSize(map<string, string>::size_type s) {
    return WriteInt(s);
  }

  Status skipToPosW(int n) {
    int toSkip = std::max(0, n - pos);

    for (int i = 0; i < toSkip; i++) {
      TF_RETURN_IF_ERROR(WriteByte(0));
    }

    return Status::OK();
  }

  Status writeBoolean(bool val) {
    return WriteByte((char) (val ? 1 : 0));
  }

  Status writeString(string str) {
    if (!str.empty()) {
      writeBoolean(false);
      unsigned short l = str.length();
      TF_RETURN_IF_ERROR(WriteShort(l));
      TF_RETURN_IF_ERROR(WriteData((uint8_t *)str.c_str(), str.length()));
    } else {
      writeBoolean(true);
    }

    return Status::OK();
  }

  Status writeStringMap(map<string, string> map) {
    std::map<string, string>::size_type size = map.size();
    TF_RETURN_IF_ERROR(writeSize(size));

    for (auto const &x : map) {
      TF_RETURN_IF_ERROR(writeString(x.first));
      TF_RETURN_IF_ERROR(writeString(x.second));
    }

    return Status::OK();
  }

  void reset() {
    pos = 0;
  }

 private:
  // Position to be read next
  int pos;
};

template<class T>
class Optional {
 public:
  template<class T1>
  static Optional<T1> of(T1 val) {
    return Optional(val);
  }

  template<class T1>
  static Optional<T1> empty() {
    return Optional();
  }

  bool isEmpty() {
    return hasVal;
  }

  T getOrElse(T altVal) {
    return isEmpty() ? altVal : val;
  }

  T get() {
    return val;
  }

  Optional() {
    hasVal = false;
  }

  Optional(T val) {
    this->val = val;
  }

  Status read(IGFSClient &r) {
    TF_RETURN_IF_ERROR(r.ReadBool(hasVal));

    if (hasVal) {
      val = T();
      TF_RETURN_IF_ERROR(val.read(r));
    }

    return Status::OK();
  }

 private:
  bool hasVal = false;
  T val;
};
}

#endif //TF_CMAKE_UTILS_H
