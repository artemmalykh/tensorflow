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

#ifndef TENSORFLOW_CONTRIB_IGFS_KERNELS_IGFS_CLIENT_H_
#define TENSORFLOW_CONTRIB_IGFS_KERNELS_IGFS_CLIENT_H_

#include <string>

#include "igfs_messages.h"
#include "igfs_utils.h"

namespace tensorflow {

class IGFSClient {
 public:
  IGFSClient(std::string host, int port, std::string fs_name);
  ~IGFSClient();
  Status Handshake(ControlResponse<Optional<HandshakeResponse>> *res);
  Status ListFiles(ControlResponse<ListFilesResponse> *res,
                   const std::string &path);
  Status ListPaths(ControlResponse<ListPathsResponse> *res,
                   const std::string &path);
  Status Info(ControlResponse<InfoResponse> *res, const std::string &path);
  Status OpenCreate(ControlResponse<OpenCreateResponse> *res,
                    const std::string &path);
  Status OpenAppend(ControlResponse<OpenAppendResponse> *res,
                    const std::string &user_name, const std::string &path);
  Status OpenRead(ControlResponse<Optional<OpenReadResponse>> *res,
                  const std::string &user_name, const std::string &path);
  Status Exists(ControlResponse<ExistsResponse> *res, const std::string &path);
  Status MkDir(ControlResponse<MakeDirectoriesResponse> *res,
               const std::string &path);
  Status Delete(ControlResponse<DeleteResponse> *res, const std::string &path,
                bool recursive);
  Status WriteBlock(int64_t stream_id, const uint8_t *data, int32_t len);
  Status ReadBlock(ReadBlockControlResponse *res, int64_t stream_id,
                   int64_t pos, int32_t length);
  Status Close(ControlResponse<CloseResponse> *res, int64_t stream_id);
  Status Rename(ControlResponse<RenameResponse> *res, const std::string &source,
                const std::string &dest);

 private:
  const std::string fs_name_;
  ExtendedTCPClient client_;
};
}  // namespace tensorflow

#endif
