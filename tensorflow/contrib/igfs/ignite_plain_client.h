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

#ifndef IGNITE_PLAIN_CLIENT_H
#define IGNITE_PLAIN_CLIENT_H
#include "tensorflow/core/lib/core/status.h"

#include <string>

namespace ignite {

class PlainClient {
 public:
  PlainClient(std::string host, int port);
  ~PlainClient();

  virtual tensorflow::Status Connect();
  virtual tensorflow::Status Disconnect();
  virtual const bool IsConnected();
  virtual const int GetSocketDescriptor();
  virtual const tensorflow::Status Ignore(int length);
  virtual const tensorflow::Status ReadData(char* buf, int length);
  virtual const tensorflow::Status WriteData(const char* buf, int length);

 private:
  std::string host;
  int port;
  int sock;
};

}  // namespace ignite

#endif