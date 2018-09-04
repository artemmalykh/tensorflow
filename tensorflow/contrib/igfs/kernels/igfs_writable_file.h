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

#include "igfs_client.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/file_system.h"

namespace tensorflow {

class IGFSWritableFile : public WritableFile {
 public:
  IGFSWritableFile(const string &file_name, long resource_id,
                   std::shared_ptr<IGFSClient> client);
  ~IGFSWritableFile() override;
  Status Append(const StringPiece &data) override;
  Status Close() override;
  Status Flush() override;
  Status Sync() override;

 private:
  const string file_name_;
  long resource_id_;
  std::shared_ptr<IGFSClient> client_;
};

}  // namespace tensorflow