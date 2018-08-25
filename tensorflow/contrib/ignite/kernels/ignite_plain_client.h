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

#ifndef TENSORFLOW_CONTRIB_IGNITE_KERNELS_IGNITE_CLIENT_H_
#define TENSORFLOW_CONTRIB_IGNITE_KERNELS_IGNITE_CLIENT_H_
#include "ignite_client.h"
#endif

#include <string>

namespace tensorflow {

class PlainClient : public Client {
 public:
  PlainClient(std::string host, int port);
  ~PlainClient();

  virtual Status Connect();
  virtual Status Disconnect();
  virtual bool IsConnected();
  virtual int GetSocketDescriptor();
  virtual Status ReadData(uint8_t* buf, int32_t length);
  virtual Status WriteData(uint8_t* buf, int32_t length);

 private:
  std::string host;
  int port;
  int sock;
};

}  // namespace tensorflow
