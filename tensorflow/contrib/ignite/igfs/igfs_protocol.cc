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

#include "igfs_protocol.h"
#include "../kernels/ignite_plain_client.h"

namespace tensorflow {

IgfsProtocolMessenger::IgfsProtocolMessenger(int port,
                                             const string &host,
                                             const string &fs_name)
    : fs_name_(fs_name) {
  cl = new IGFSClient(host, port);
  cl->Connect();
}

IgfsProtocolMessenger::~IgfsProtocolMessenger() {
  cl->Disconnect();
}

Status IgfsProtocolMessenger::handshake(
    ControlResponse<Optional<HandshakeResponse>>* res) {
  LOG(INFO) << "Handshake.";
  HandshakeRequest req(fs_name_, "");
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  LOG(INFO) << "Handshake OK.";
  return Status::OK();
}

Status IgfsProtocolMessenger::listFiles(ControlResponse<ListFilesResponse>* res,
                                        const string &path) {
  ListFilesRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::listPaths(ControlResponse<ListPathsResponse>* res,
                                        const string &path) {
  ListPathsRequest req(path);
  req.write(*cl);
  cl->reset();

  res->Read(*cl);
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::info(ControlResponse<InfoResponse>* res,
                                   const string &path) {
  InfoRequest req("", path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::openCreate(
    ControlResponse<OpenCreateResponse>* res,
    const string &path) {
  OpenCreateRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::openAppend(
    ControlResponse<OpenAppendResponse>* res,
    const string &userName,
    const string &path) {
  OpenAppendRequest req(userName, path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::openRead(
    ControlResponse<Optional<OpenReadResponse>>* res,
    const string &user_name,
    const string &path) {
  OpenReadRequest req(user_name, path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::exists(ControlResponse<ExistsResponse>* res,
                                     const string &path) {
  ExistsRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::mkdir(
    ControlResponse<MakeDirectoriesResponse>* res,
    const string &path) {
  MakeDirectoriesRequest req("", path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::del(ControlResponse<DeleteResponse>* res,
                                  const string &path,
                                  bool recursive) {
  DeleteRequest req(path, recursive);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::writeBlock(long stream_id,
                                         const char *data,
                                         int len) {
  WriteBlockRequest req(stream_id, data, len);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::readBlock(ReadBlockControlResponse* res,
                                        long stream_id,
                                        long pos,
                                        int length,
                                        char* dst) {
  ReadBlockRequest req(stream_id, pos, length);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::close(ControlResponse<CloseResponse>* res,
                                    long stream_id) {
  CloseRequest req(stream_id);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::rename(ControlResponse<RenameResponse>* res,
                                     const string &source,
                                     const string &dest) {
  RenameRequest req(source, dest);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res->Read(*cl));
  cl->reset();

  return Status::OK();
}
}
