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

namespace tensorflow {

class Client {
 public:
  Client(bool big_endian);
  virtual Status Connect() = 0;
  virtual Status Disconnect() = 0;
  virtual bool IsConnected() = 0;
  virtual int GetSocketDescriptor() = 0;
  virtual Status ReadData(uint8_t* buf, int32_t length) = 0;
  virtual Status WriteData(uint8_t* buf, int32_t length) = 0;

  inline Status ReadByte(uint8_t* data) { return ReadData(data, 1); }

  inline Status ReadShort(int16_t *data) {
    TF_RETURN_IF_ERROR(ReadData((uint8_t *)data, 2));
    swap_if_required16(data);

    return Status::OK();
  }

  inline Status ReadInt(int32_t *data) {
    TF_RETURN_IF_ERROR(ReadData((uint8_t *)data, 4));
    swap_if_required32(data);

    return Status::OK();
  }

  inline Status ReadLong(int64_t *data) {
    TF_RETURN_IF_ERROR(ReadData((uint8_t *)data, 8));
    swap_if_required64(data);

    return Status::OK();
  }

  inline Status WriteByte(uint8_t data) { return WriteData(&data, 1); }

  inline Status WriteShort(int16_t data) {
    int16_t tmp = data;
    swap_if_required16(&tmp);
    return WriteData((uint8_t *)&tmp, 2);
  }

  inline Status WriteInt(int32_t data) {
    int32_t tmp = data;
    swap_if_required32(&tmp);
    return WriteData((uint8_t *)&tmp, 4);
  }

  inline Status WriteLong(int64_t data) {
    int64_t tmp = data;
    swap_if_required64(&tmp);
    return WriteData((uint8_t *)&tmp, 8);
  }

 private:
  bool swap_;

  inline void swap_if_required16(int16_t *x) {
    if (swap_) {
      *x = ((*x & 0xFF) << 8) | ((*x >> 8) & 0xFF);
    }
  }

  inline void swap_if_required32(int32_t *x) {
    if (swap_) {
      *x = ((*x & 0xFF) << 24) | (((*x >> 8) & 0xFF) << 16) |
           (((*x >> 16) & 0xFF) << 8) | ((*x >> 24) & 0xFF);
    }
  }

  inline void swap_if_required64(int64_t *x) {
    if (swap_) {
      *x = ((*x & 0xFF) << 56) | (((*x >> 8) & 0xFF) << 48) |
           (((*x >> 16) & 0xFF) << 40) | (((*x >> 24) & 0xFF) << 32) |
           (((*x >> 32) & 0xFF) << 24) | (((*x >> 40) & 0xFF) << 16) |
           (((*x >> 48) & 0xFF) << 8) | ((*x >> 56) & 0xFF);
    }
  }
};

}  // namespace tensorflow

#endif
