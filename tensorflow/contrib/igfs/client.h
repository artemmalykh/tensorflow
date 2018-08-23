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
#include <cstdio>
#include <sys/socket.h>
#include <cstdlib>
#include <netinet/in.h>
#include <cstring>
#include <arpa/inet.h>
#include <iostream>
#include <bits/unique_ptr.h>
#include "utils.h"
#include "messages.h"
#include "ignite_plain_client.h"


using namespace std;

namespace tensorflow {
class Network : public std::exception {
 public:
  Network(const string& s) {
    cause = s.c_str();
  }

  const char *what() {
    return cause;
  }

 private:
  const char *cause;
};

class IgfsClient {
 public:
  IgfsClient(int port, string host, string fsName);

  ControlResponse<Optional<HandshakeResponse>> handshake();

  ControlResponse<ListFilesResponse> listFiles(string path);

  ControlResponse<ListPathsResponse> listPaths(string path);

  ControlResponse<InfoResponse> info(string path);

  ControlResponse<OpenCreateResponse> openCreate(string& path);

  ControlResponse<OpenAppendResponse> openAppend(string userName, string path);

  ControlResponse<Optional<OpenReadResponse>> openRead(string userName, string path);

  ControlResponse<ExistsResponse> exists(const string& path);

  ControlResponse<MakeDirectoriesResponse> mkdir(const string& path);

  ControlResponse<DeleteResponse> del(string path, bool recursive);

  void writeBlock(long streamId, const char *data, int len);

  ReadBlockControlResponse readBlock(long streamId, long pos, int len, char* dst);

  ControlResponse<CloseResponse> close(long streamId);

  ControlResponse<RenameResponse> rename(const string &source, const string &dest);

 private:
  ignite::PlainClient plainClient;
  string fsName;

  Writer *w;
  Reader *r;

  Writer *createWriter();
  Reader *createReader();
};
}

#endif //TF_CMAKE_CLIENT_H
