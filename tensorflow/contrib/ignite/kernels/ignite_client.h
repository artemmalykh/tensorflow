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

#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"

#ifdef _MSC_VER

#include <stdlib.h>
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#else

#include <byteswap.h>

#endif

namespace tensorflow {

class Client {
 public:
  Client() : Client(false) {};

  explicit Client(bool bigEndian)  {
    int x = 1;
    bool isLittleEndian = (*(char *)&x == 1);
    shouldSwap_ = (bigEndian == isLittleEndian);
  };

  virtual Status Connect() = 0;

  virtual Status Disconnect() = 0;

  virtual bool IsConnected() = 0;

  virtual int GetSocketDescriptor() = 0;

  virtual Status ReadData(uint8_t *buf, int32_t length) = 0;

  virtual Status WriteData(uint8_t *buf, int32_t length) = 0;

  inline Status ReadByte(uint8_t *data) {
    return ReadData(data, 1);
  }

  inline Status ReadShort(int16_t *data) {
    TF_RETURN_IF_ERROR(ReadData((uint8_t *) data, 2));
    *data = shouldSwap_ ? bswap_16(*data) : *data;
    return Status::OK();
  }

  inline Status ReadInt(int32_t *data) {
    TF_RETURN_IF_ERROR(ReadData((uint8_t *) data, 4));
    *data = shouldSwap_ ? bswap_32(*data) : *data;
    return Status::OK();
  }

  inline Status ReadLong(int64_t *data) {
    TF_RETURN_IF_ERROR(ReadData((uint8_t *) data, 8));
    *data = shouldSwap_ ? bswap_64(*data) : *data;
    return Status::OK();
  }

  inline Status WriteByte(uint8_t data) { return WriteData(&data, 1); }

  inline Status WriteShort(int16_t data) {
    int16_t d = shouldSwap_ ? bswap_16(data) : data;
    return WriteData((uint8_t *) &d, 2);
  }

  inline Status WriteInt(int32_t data) {
    int32_t d = shouldSwap_ ? bswap_32(data) : data;
    return WriteData((uint8_t *) &d, 4);
  }

  inline Status WriteLong(int64_t data) {
    int64_t d = shouldSwap_ ? bswap_64(data) : data;
    return WriteData((uint8_t *) &d, 8);
  }

 protected:
  bool shouldSwap_;
};

}  // namespace tensorflow

#endif
