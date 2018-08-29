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
#ifndef IGNITE_CLIENT_H
#define IGNITE_CLIENT_H

#include "tensorflow/core/lib/core/status.h"

namespace ignite {

class Client {
 public:
  explicit Client(bool bigEndian);
  Client() : Client(false) {};

  virtual tensorflow::Status Connect() = 0;
  virtual tensorflow::Status Disconnect() = 0;
  virtual const bool IsConnected() = 0;
  virtual const int GetSocketDescriptor() = 0;

  virtual tensorflow::Status ReadByte(uint8_t& data);
  virtual tensorflow::Status ReadShort(int16_t& data);
  virtual tensorflow::Status ReadInt(int32_t& data);
  virtual tensorflow::Status ReadLong(int64_t& data);
  virtual tensorflow::Status ReadData(uint8_t* buf, int32_t length) = 0;

  virtual tensorflow::Status WriteByte(uint8_t data);
  virtual tensorflow::Status WriteShort(int16_t data);
  virtual tensorflow::Status WriteInt(int32_t data);
  virtual tensorflow::Status WriteLong(int64_t data);
  virtual tensorflow::Status WriteData(const uint8_t* buf, int32_t length) = 0;
 protected:
  template <typename A, int N> tensorflow::Status Read(A d);
  bool bigEndian;
  bool shouldSwap;
};

}  // namespace ignite
#endif