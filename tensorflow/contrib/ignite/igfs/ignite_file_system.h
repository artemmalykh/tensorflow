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

#ifndef TF_CONTRIB_IGNITE_FILE_SYSTEM_H
#define TF_CONTRIB_IGNITE_FILE_SYSTEM_H

#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system.h"
#include "utils.h"

using namespace std;

namespace tensorflow {

class IgniteFileSystem : public FileSystem {
 public:
  IgniteFileSystem();
  ~IgniteFileSystem();

  Status NewRandomAccessFile(
      const string& fname, std::unique_ptr<RandomAccessFile>* result) override;

  Status NewWritableFile(const string& fname,
                         std::unique_ptr<WritableFile>* result) override;

  Status NewAppendableFile(const string& fname,
                           std::unique_ptr<WritableFile>* result) override;

  Status NewReadOnlyMemoryRegionFromFile(
      const string& fname,
      std::unique_ptr<ReadOnlyMemoryRegion>* result) override;

  Status FileExists(const string& fname) override;

  Status GetChildren(const string& dir, std::vector<string>* result) override;

  Status GetMatchingPaths(const string& pattern,
                          std::vector<string>* results) override;

  Status DeleteFile(const string& fname) override;

  Status CreateDir(const string& name) override;

  Status DeleteDir(const string& name) override;

  Status GetFileSize(const string& fname, uint64* size) override;

  Status RenameFile(const string& src, const string& target) override;

  Status Stat(const string& fname, FileStatistics* stat) override;

  string TranslateName(const string& name) const override;

 private:
  Status Connect(StringPiece fname);
};

}  // namespace tensorflow


#endif //TF_CMAKE_IGNITE_FILE_SYSTEM_H
