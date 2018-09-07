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

#ifndef TENSORFLOW_CONTRIB_IGFS_KERNELS_IGFS_EXTENDED_TCP_CLIENT_H_
#define TENSORFLOW_CONTRIB_IGFS_KERNELS_IGFS_EXTENDED_TCP_CLIENT_H_

#include "tensorflow/contrib/ignite/kernels/ignite_plain_client.h"

using std::map;
using std::string;
#include <sys/socket.h>
#include <iostream>
#include <map>
#include <memory>
#include <streambuf>
#include <string>

namespace tensorflow {
	
class ExtendedTCPClient : public PlainClient {
 public:
  ExtendedTCPClient(const std::string &host, int port);
  Status ReadData(uint8_t *buf, int32_t length) override;
  Status WriteData(uint8_t *buf, int32_t length) override;
  Status Skip(int n);
  Status Ignore(int n);
  Status SkipToPos(int targetPos);
  Status ReadBool(bool &res);
  Status ReadNullableString(std::string &res);
  Status ReadString(std::string &res);
  Status ReadStringMap(std::map<std::string, std::string> &res);
  Status WriteSize(std::map<std::string, std::string>::size_type s);
  Status FillWithZerosUntil(int n);
  Status WriteBool(bool val);
  Status WriteString(std::string str);
  Status WriteStringMap(std::map<std::string, std::string> map);
  void reset();

 private:
  int pos_;
};

}  // namespace tensorflow

#endif