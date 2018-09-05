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

namespace tensorflow {

IGFSClient::IGFSClient(string host, int port, string fs_name)
    : fs_name_(fs_name), client_(ExtendedTCPClient(host, port)) {
  client_.Connect();
}

IGFSClient::~IGFSClient() { client_.Disconnect(); }

Status IGFSClient::SendRequestGetResponse(const std::string request_name, const Request &request, Response *response) {
  TF_RETURN_IF_ERROR(request.Write(&client_));
  client_.reset();

  if (response != nullptr) {
    TF_RETURN_IF_ERROR(response->Read(&client_));
    client_.reset();
  }

  LOG(INFO) << "IGFS '" << request_name << "' successfully completed";

  return Status::OK();
}

Status IGFSClient::Handshake(
    CtrlResponse<Optional<HandshakeResponse>> *response) {
  return SendRequestGetResponse(
    "handshake",
    HandshakeRequest(fs_name_, ""), 
    response
  );
}

Status IGFSClient::ListFiles(CtrlResponse<ListFilesResponse> *response,
                             const string &path) {
  return SendRequestGetResponse(
    "list files",
    ListFilesRequest(path), 
    response
  );
}

Status IGFSClient::ListPaths(CtrlResponse<ListPathsResponse> *response,
                             const string &path) {
  return SendRequestGetResponse(
    "list paths",
    ListPathsRequest(path), 
    response
  );
}

Status IGFSClient::Info(CtrlResponse<InfoResponse> *response,
                        const string &path) {
  return SendRequestGetResponse(
    "info",
    InfoRequest("", path), 
    response
  );
}

Status IGFSClient::OpenCreate(CtrlResponse<OpenCreateResponse> *response,
                              const string &path) {
  return SendRequestGetResponse(
    "open create",
    OpenCreateRequest(path), 
    response
  );
}

Status IGFSClient::OpenAppend(CtrlResponse<OpenAppendResponse> *response,
                              const string &user_name, const string &path) {
  return SendRequestGetResponse(
    "open append",
    OpenAppendRequest(user_name, path), 
    response
  );
}

Status IGFSClient::OpenRead(CtrlResponse<Optional<OpenReadResponse>> *response,
                            const string &user_name, const string &path) {
  return SendRequestGetResponse(
    "open read",
    OpenReadRequest(user_name, path), 
    response
  );
}

Status IGFSClient::Exists(CtrlResponse<ExistsResponse> *response,
                          const string &path) {
  return SendRequestGetResponse(
    "exists",
    ExistsRequest(path), 
    response
  );
}

Status IGFSClient::MkDir(CtrlResponse<MakeDirectoriesResponse> *response,
                         const string &path) {
  return SendRequestGetResponse(
    "mkdir",
    MakeDirectoriesRequest("", path), 
    response
  );
}

Status IGFSClient::Delete(CtrlResponse<DeleteResponse> *response,
                          const string &path, bool recursive) {
  return SendRequestGetResponse(
    "delete",
    DeleteRequest(path, recursive), 
    response
  );
}

Status IGFSClient::WriteBlock(int64_t stream_id, const uint8_t *data,
                              int32_t len) {
  return SendRequestGetResponse(
    "write block",
    WriteBlockRequest(stream_id, data, len), 
    nullptr
  );
}

Status IGFSClient::ReadBlock(ReadBlockCtrlResponse *response, int64_t stream_id,
                             int64_t pos, int32_t length) {
  return SendRequestGetResponse(
    "read block",
    ReadBlockRequest(stream_id, pos, length), 
    response
  );
}

Status IGFSClient::Close(CtrlResponse<CloseResponse> *response,
                         int64_t stream_id) {
  return SendRequestGetResponse(
    "close",
    CloseRequest(stream_id), 
    response
  );
}

Status IGFSClient::Rename(CtrlResponse<RenameResponse> *response,
                          const string &source, const string &dest) {
  return SendRequestGetResponse(
    "rename",
    RenameRequest(source, dest), 
    response
  );
}

}  // namespace tensorflow
