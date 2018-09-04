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

Status IGFSClient::Handshake(
    ControlResponse<Optional<HandshakeResponse>> *res) {
  HandshakeRequest req(fs_name_, "");

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'handshake' successfully completed [fs_name=" << fs_name_
            << "]";

  return Status::OK();
}

Status IGFSClient::ListFiles(ControlResponse<ListFilesResponse> *res,
                             const string &path) {
  ListFilesRequest req(path);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'list files' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::ListPaths(ControlResponse<ListPathsResponse> *res,
                             const string &path) {
  ListPathsRequest req(path);

  req.Write(&client_);
  client_.reset();

  res->Read(&client_);
  client_.reset();

  LOG(INFO) << "IGFS 'list paths' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::Info(ControlResponse<InfoResponse> *res,
                        const string &path) {
  InfoRequest req("", path);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'info' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::OpenCreate(ControlResponse<OpenCreateResponse> *res,
                              const string &path) {
  OpenCreateRequest req(path);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'open create' successfully complated [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::OpenAppend(ControlResponse<OpenAppendResponse> *res,
                              const string &user_name, const string &path) {
  OpenAppendRequest req(user_name, path);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'open append' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::OpenRead(ControlResponse<Optional<OpenReadResponse>> *res,
                            const string &user_name, const string &path) {
  OpenReadRequest req(user_name, path);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'open read' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::Exists(ControlResponse<ExistsResponse> *res,
                          const string &path) {
  ExistsRequest req(path);
  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  return Status::OK();
}

Status IGFSClient::MkDir(ControlResponse<MakeDirectoriesResponse> *res,
                         const string &path) {
  MakeDirectoriesRequest req("", path);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'mkdir' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::Delete(ControlResponse<DeleteResponse> *res,
                          const string &path, bool recursive) {
  DeleteRequest req(path, recursive);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'del' successfully completed [fs_name=" << fs_name_
            << ", path=" << path << "]";

  return Status::OK();
}

Status IGFSClient::WriteBlock(int64_t stream_id, const uint8_t *data,
                              int32_t len) {
  WriteBlockRequest req(stream_id, data, len);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'write block' successfully completed";

  return Status::OK();
}

Status IGFSClient::ReadBlock(ReadBlockControlResponse *res, int64_t stream_id,
                             int64_t pos, int32_t length) {
  ReadBlockRequest req(stream_id, pos, length);
  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'read block' successfully completed";

  return Status::OK();
}

Status IGFSClient::Close(ControlResponse<CloseResponse> *res,
                         int64_t stream_id) {
  CloseRequest req(stream_id);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'close' successfully completed";

  return Status::OK();
}

Status IGFSClient::Rename(ControlResponse<RenameResponse> *res,
                          const string &source, const string &dest) {
  RenameRequest req(source, dest);

  TF_RETURN_IF_ERROR(req.Write(&client_));
  client_.reset();

  TF_RETURN_IF_ERROR(res->Read(&client_));
  client_.reset();

  LOG(INFO) << "IGFS 'rename' successfully completed [src=" << source
            << ", dst=" << dest << "]";

  return Status::OK();
}

}  // namespace tensorflow
