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

#include "igfs_writable_file.h"
#include "igfs_messages.h"

namespace tensorflow {

IGFSWritableFile::IGFSWritableFile(const string &file_name, long resource_id,
                                   std::shared_ptr<IGFSClient> client)
    : file_name_(file_name), resource_id_(resource_id), client_(client) {
  LOG(INFO) << "Construct new writable file " << file_name;
}

IGFSWritableFile::~IGFSWritableFile() {
  if (resource_id_ >= 0) {
    CtrlResponse<CloseResponse> cr = {};
    client_->Close(&cr, resource_id_);
  }
}

Status IGFSWritableFile::Append(const StringPiece &data) {
  LOG(INFO) << "Append to " << file_name_;
  TF_RETURN_IF_ERROR(
      client_->WriteBlock(resource_id_, (uint8_t *)data.data(), data.size()));

  return Status::OK();
}

Status IGFSWritableFile::Close() {
  LOG(INFO) << "Close " << file_name_;
  Status result;

  CtrlResponse<CloseResponse> cr = {};
  client_->Close(&cr, resource_id_);

  resource_id_ = -1;

  return result;
}

Status IGFSWritableFile::Flush() { return Status::OK(); }

Status IGFSWritableFile::Sync() { return Status::OK(); }

}  // namespace tensorflow