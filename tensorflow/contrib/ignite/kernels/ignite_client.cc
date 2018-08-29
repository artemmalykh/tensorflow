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

#include "ignite_client.h"
#include <byteswap.h>

namespace ignite {

Client::Client(bool bigEndian) : bigEndian(bigEndian) {
  int x = 1;
  bool isLittleEndian = (*(char *)&x == 1);
  Client::shouldSwap = bigEndian == isLittleEndian;
}

template <typename A, int Z> tensorflow::Status Client::Read(A d) {
  const tensorflow::Status res = ReadData((uint8_t*)&d, Z);
  if (Client::shouldSwap) {
    switch(Z) {
      case 2:
        d = bswap_16(d);
        break;
      case 4:
        d = bswap_32(d);
        break;
      case 8:
        d = bswap_64(d);
        break;
      default:
        d = bswap_16(d);
    }
  }
  return res;
};

tensorflow::Status Client::ReadByte(uint8_t& data) {
  return ReadData((uint8_t*)&data, 1);
}

tensorflow::Status Client::ReadShort(int16_t& data) {
  return Read<int16_t&, 2>(data);
}

tensorflow::Status Client::ReadInt(int32_t& data) {
  return Read<int32_t&, 4>(data);
}

tensorflow::Status Client::ReadLong(int64_t& data) {
  return Read<int64_t&, 8>(data);
}

tensorflow::Status Client::WriteByte(uint8_t data) {
  return WriteData((uint8_t*)&data, 1);
}

tensorflow::Status Client::WriteShort(int16_t data) {
  int16_t d = shouldSwap ? bswap_16(data) : data;
  return WriteData((uint8_t*)&d, 2);
}

tensorflow::Status Client::WriteInt(int32_t data) {
  int32_t d = shouldSwap ? bswap_32(data) : data;
  return WriteData((uint8_t*)&d, 4);
}

tensorflow::Status Client::WriteLong(int64_t data) {
  int16_t d = shouldSwap ? bswap_64(data) : data;
  return WriteData((uint8_t*)&d, 8);
}

}  // namespace ignite
