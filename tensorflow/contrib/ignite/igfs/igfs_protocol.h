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
  IgfsProtocolMessenger(int port, const string &host, const string &fs_name);

  ~IgfsProtocolMessenger();

  Status handshake(ControlResponse<Optional<HandshakeResponse>>* res);

  Status listFiles(ControlResponse<ListFilesResponse>* res, const string &path);

  Status listPaths(ControlResponse<ListPathsResponse>* res, const string &path);

  Status info(ControlResponse<InfoResponse> *res, const string &path);

  Status openCreate(ControlResponse<OpenCreateResponse> *res, const string &path);

  Status openAppend(ControlResponse<OpenAppendResponse> *res, const string &userName, const string &path);

  Status openRead(ControlResponse<Optional<OpenReadResponse>> *res, const string &user_name, const string &path);

  Status exists(ControlResponse<ExistsResponse> *res, const string &path);

  Status mkdir(ControlResponse<MakeDirectoriesResponse> *res, const string &path);

  Status del(ControlResponse<DeleteResponse> *res, const string &path, bool recursive);

  Status writeBlock(long stream_id, const char *data, int len);

  Status readBlock(ReadBlockControlResponse *res, long stream_id, long pos, int length, char *dst);

  Status close(ControlResponse<CloseResponse> *res, long stream_id);

  Status rename(ControlResponse<RenameResponse> *res, const string &source, const string &dest);

 private:
  string fs_name_;
  IGFSClient *cl;
};
} // namespace tensorflow

#endif //TF_CMAKE_CLIENT_H
