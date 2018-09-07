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

#include "igfs_extended_tcp_client.h"

namespace tensorflow {

ExtendedTCPClient::ExtendedTCPClient(const std::string &host, int port) : PlainClient(host, port, true), pos_(0) {}

Status ExtendedTCPClient::ReadData(uint8_t *buf, int32_t length) {
  TF_RETURN_IF_ERROR(PlainClient::ReadData(buf, length));
  pos_ += length;

  return Status::OK();
}

Status ExtendedTCPClient::WriteData(uint8_t *buf, int32_t length) {
  TF_RETURN_IF_ERROR(PlainClient::WriteData(buf, length));
  pos_ += length;

  return Status::OK();
}

Status ExtendedTCPClient::Skip(int n) {
  TF_RETURN_IF_ERROR(Ignore(n));

  return Status::OK();
};

Status ExtendedTCPClient::Ignore(int n) {
  uint8_t buf[n];
  return ReadData(buf, n);
}

Status ExtendedTCPClient::SkipToPos(int targetPos) {
  int toSkip = std::max(0, targetPos - pos_);
  return Ignore(toSkip);
};

Status ExtendedTCPClient::ReadBool(bool &res) {
  uint8_t d = 0;
  TF_RETURN_IF_ERROR(ReadData(&d, 1));
  res = d != 0;

  return Status::OK();
}

Status ExtendedTCPClient::ReadNullableString(std::string &res) {
  bool isEmpty = false;
  ReadBool(isEmpty);

  if (isEmpty) {
    res = string();
  } else {
    TF_RETURN_IF_ERROR(ReadString(res));
  }

  return Status::OK();
}

Status ExtendedTCPClient::ReadString(std::string &res) {
  short len;
  TF_RETURN_IF_ERROR(ReadShort(&len));
  auto *cStr = new uint8_t[len + 1];
  cStr[len] = 0;
  TF_RETURN_IF_ERROR(ReadData(cStr, len));
  res = string((char *)cStr);

  return Status::OK();
}

Status ExtendedTCPClient::ReadStringMap(std::map<std::string, std::string> &res) {
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

Status ExtendedTCPClient::WriteSize(std::map<std::string, std::string>::size_type s) { return WriteInt(s); }

Status ExtendedTCPClient::FillWithZerosUntil(int n) {
  int toSkip = std::max(0, n - pos_);

  for (int i = 0; i < toSkip; i++) {
    TF_RETURN_IF_ERROR(WriteByte(0));
  }

  return Status::OK();
}

Status ExtendedTCPClient::WriteBool(bool val) { return WriteByte((char)(val ? 1 : 0)); }

Status ExtendedTCPClient::WriteString(std::string str) {
  if (!str.empty()) {
    WriteBool(false);
    unsigned short l = str.length();
    TF_RETURN_IF_ERROR(WriteShort(l));
    TF_RETURN_IF_ERROR(WriteData((uint8_t *)str.c_str(), str.length()));
  } else {
    WriteBool(true);
  }

  return Status::OK();
}

Status ExtendedTCPClient::WriteStringMap(std::map<std::string, std::string> map) {
  std::map<string, string>::size_type size = map.size();
  TF_RETURN_IF_ERROR(WriteSize(size));

  for (auto const &x : map) {
    TF_RETURN_IF_ERROR(WriteString(x.first));
    TF_RETURN_IF_ERROR(WriteString(x.second));
  }

  return Status::OK();
}

void ExtendedTCPClient::reset() { pos_ = 0; }

}  // namespace tensorflow