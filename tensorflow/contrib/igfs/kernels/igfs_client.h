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
  IGFSClient(std::string host, int port, std::string fs_name, std::string user_name);
  ~IGFSClient();
  Status Handshake(CtrlResponse<Optional<HandshakeResponse>> *res);
  Status ListFiles(CtrlResponse<ListFilesResponse> *res,
                   const std::string &path);
  Status ListPaths(CtrlResponse<ListPathsResponse> *res,
                   const std::string &path);
  Status Info(CtrlResponse<InfoResponse> *res, const std::string &path);
  Status OpenCreate(CtrlResponse<OpenCreateResponse> *res,
                    const std::string &path);
  Status OpenAppend(CtrlResponse<OpenAppendResponse> *res,
                    const std::string &path);
  Status OpenRead(CtrlResponse<Optional<OpenReadResponse>> *res,
                  const std::string &path);
  Status Exists(CtrlResponse<ExistsResponse> *res, const std::string &path);
  Status MkDir(CtrlResponse<MakeDirectoriesResponse> *res,
               const std::string &path);
  Status Delete(CtrlResponse<DeleteResponse> *res, const std::string &path,
                bool recursive);
  Status WriteBlock(int64_t stream_id, const uint8_t *data, int32_t len);
  Status ReadBlock(ReadBlockCtrlResponse *res, int64_t stream_id,
                   int64_t pos, int32_t length);
  Status Close(CtrlResponse<CloseResponse> *res, int64_t stream_id);
  Status Rename(CtrlResponse<RenameResponse> *res, const std::string &source,
                const std::string &dest);

 private:
  const std::string fs_name_;
  const std::string user_name_;
  ExtendedTCPClient client_;

  Status SendRequestGetResponse(const std::string request_name, const Request &request, Response *response);
};
}  // namespace tensorflow

#endif
