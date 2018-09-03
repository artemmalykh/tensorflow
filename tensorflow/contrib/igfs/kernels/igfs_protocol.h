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

#ifndef TF_CMAKE_CLIENT_H
#define TF_CMAKE_CLIENT_H

#include <string>
#include "messages.h"
#include "utils.h"

namespace tensorflow {

class IgfsProtocolMessenger {
 public:
  IgfsProtocolMessenger(int32_t port, const string &host,
                        const string &fs_name);

  ~IgfsProtocolMessenger();

  Status Handshake(ControlResponse<Optional<HandshakeResponse>> *res);

  Status ListFiles(ControlResponse<ListFilesResponse> *res, const string &path);

  Status ListPaths(ControlResponse<ListPathsResponse> *res, const string &path);

  Status Info(ControlResponse<InfoResponse> *res, const string &path);

  Status OpenCreate(ControlResponse<OpenCreateResponse> *res,
                    const string &path);

  Status OpenAppend(ControlResponse<OpenAppendResponse> *res,
                    const string &user_name, const string &path);

  Status OpenRead(ControlResponse<Optional<OpenReadResponse>> *res,
                  const string &user_name, const string &path);

  Status Exists(ControlResponse<ExistsResponse> *res, const string &path);

  Status MkDir(ControlResponse<MakeDirectoriesResponse> *res,
               const string &path);

  Status Del(ControlResponse<DeleteResponse> *res, const string &path,
             bool recursive);

  Status WriteBlock(int64_t stream_id, const uint8_t *data, int32_t len);

  Status
  ReadBlock(ReadBlockControlResponse *res, int64_t stream_id, int64_t pos,
            int32_t length);

  Status Close(ControlResponse<CloseResponse> *res, int64_t stream_id);

  Status Rename(ControlResponse<RenameResponse> *res, const string &source,
                const string &dest);

 private:
  string fs_name_;
  IGFSClient *cl;
};
} // namespace tensorflow

#endif //TF_CMAKE_CLIENT_H
