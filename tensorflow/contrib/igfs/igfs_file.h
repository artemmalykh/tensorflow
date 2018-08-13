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

#ifndef TF_IGFS_IGFS_FILE_H
#define TF_IGFS_IGFS_FILE_H

#include "utils.h"

//class IgfsFile {
// public:
//  IgfsFile() {
//
//  }
//
//  void read(Reader& r) {
//    path = r.readNullableString();
//    blockSize = r.readInt();
//    groupBlockSize = r.readLong();
//    len = r.readLong();
//    props = r.readStringMap();
//    accessTime = r.readLong();
//    modificationTime = r.readLong();
//    flags = r.readChar();
//  }
//
//  const std::string &getPath() const {
//    return path;
//  }
//
//  int getBlockSize() const {
//    return blockSize;
//  }
//
//  long getGroupBlockSize() const {
//    return groupBlockSize;
//  }
//
//  const std::map<std::string, std::string> &getProps() const {
//    return props;
//  }
//
//  long getAccessTime() const {
//    return accessTime;
//  }
//
//  long getModificationTime() const {
//    return modificationTime;
//  }
//
//  char getFlags() const {
//    return flags;
//  }
//
//  long getLength() const {
//    return len;
//  }
//
// private:
//  std::string path;
//  int blockSize;
//  long groupBlockSize;
//  std::map<std::string, std::string> props;
//  unsigned long accessTime;
//  unsigned long modificationTime;
//  char flags;
//  long len;
//};

#endif //TF_CMAKE_IGFS_FILE_H
