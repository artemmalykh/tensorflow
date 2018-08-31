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

namespace ignite {

IgfsProtocolMessenger::IgfsProtocolMessenger(int port, std::string host, std::string fsName) : fsName(fsName) {
  cl = new IGFSClient(host, port);
  cl->Connect();
}

IgfsProtocolMessenger::~IgfsProtocolMessenger() {
  cl->Disconnect();
}

Status IgfsProtocolMessenger::handshake(ControlResponse<Optional<HandshakeResponse>>& res) {
  HandshakeRequest req(fsName, "");
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  std::cout << "response fs: " << res.getRes().get().getFSName() << std::endl;

  return Status::OK();
}

Status IgfsProtocolMessenger::listFiles(ControlResponse<ListFilesResponse>& res, std::string path) {
  ListFilesRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::listPaths(ControlResponse<ListPathsResponse> &res, std::string path) {
  ListPathsRequest req(path);
  req.write(*cl);
  cl->reset();

  res.read(*cl);
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::info(ControlResponse<InfoResponse> &res, std::string path) {
  InfoRequest req("", path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::openCreate(ControlResponse<OpenCreateResponse> &res, std::string& path) {
  OpenCreateRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  if (res.isOk()) {
    std::cout << "response stream id: " << res.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << res.getErrorCode() << res.getError() << std::endl;
  }

  return Status::OK();
}

Status IgfsProtocolMessenger::openAppend(ControlResponse<OpenAppendResponse> &res, std::string userName, std::string path) {
  OpenAppendRequest req(userName, path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  if (res.isOk()) {
    std::cout << "response stream id: " << res.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << res.getErrorCode() << res.getError() << std::endl;
  }

  return Status::OK();
}

Status IgfsProtocolMessenger::openRead(ControlResponse<Optional<OpenReadResponse>> &res, std::string userName, std::string path) {
  OpenReadRequest req(userName, path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::exists(ControlResponse<ExistsResponse> &res, const std::string& path) {
  ExistsRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::mkdir(ControlResponse<MakeDirectoriesResponse> &res, const std::string& path) {
  MakeDirectoriesRequest req("", path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::del(ControlResponse<DeleteResponse> &res, std::string path, bool recursive) {
  DeleteRequest req(path, recursive);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::writeBlock(long streamId, const char *data, int len) {
  WriteBlockRequest req(streamId, data, len);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::readBlock(ReadBlockControlResponse &res, long streamId, long pos, int len, char* dst) {
  ReadBlockRequest req(streamId, pos, len);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::close(ControlResponse<CloseResponse> &res, long streamId) {
  CloseRequest req(streamId);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsProtocolMessenger::rename(ControlResponse<RenameResponse> &res, const std::string &source, const std::string &dest) {
  RenameRequest req(source, dest);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}
}