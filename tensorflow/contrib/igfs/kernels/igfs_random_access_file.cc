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

#include "igfs_random_access_file.h"
#include "igfs_messages.h"

namespace tensorflow {

IGFSRandomAccessFile::IGFSRandomAccessFile(string file_name, long resource_id,
                                           std::shared_ptr<IGFSClient> &client)
    : file_name_(file_name),
      resource_id_(resource_id),
      client_(client) {}

IGFSRandomAccessFile::~IGFSRandomAccessFile() {
  ControlResponse<CloseResponse> cr = {};
  client_->Close(&cr, resource_id_);
}

Status IGFSRandomAccessFile::Read(uint64 offset, size_t n, StringPiece *result,
                                  char *scratch) const {
  LOG(INFO) << "Read " << file_name_ << " file";
  Status s;
  uint8_t *dst = reinterpret_cast<uint8_t *>(scratch);

  ReadBlockControlResponse response = ReadBlockControlResponse(dst);
  TF_RETURN_IF_ERROR(client_->ReadBlock(&response, resource_id_, offset, n));

  if (!response.IsOk()) {
    s = Status(error::INTERNAL, "Error while trying to read block.");
  } else {
    streamsize sz = response.GetRes().GetSuccessfulyRead();
    *result = StringPiece(scratch, sz);
  }

  return s;
}

}  // namespace tensorflow