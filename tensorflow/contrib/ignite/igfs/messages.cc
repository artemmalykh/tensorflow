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

#include "messages.h"

using namespace tensorflow;
using namespace ignite;

Status IgfsFile::read(IGFSClient &r) {
  path_ = Optional<IgnitePath>();
  TF_RETURN_IF_ERROR(path_.read(r));
  props_ = map<string, string>();

  TF_RETURN_IF_ERROR(r.ReadInt(&blockSize_));
  TF_RETURN_IF_ERROR(r.ReadLong(&grpBlockSize_));
  TF_RETURN_IF_ERROR(r.ReadLong(&len_));
  TF_RETURN_IF_ERROR(r.ReadStringMap(props_));
  TF_RETURN_IF_ERROR(r.ReadLong(&accessTime_));
  TF_RETURN_IF_ERROR(r.ReadLong(&modificationTime_));
  TF_RETURN_IF_ERROR(r.ReadByte((uint8_t *)&flags_));

  return Status::OK();
}

long IgfsFile::getFileSize() {
  return len_;
}

long IgfsFile::getModificationTime() {
  return modificationTime_;
}

char IgfsFile::getFlags() {
  return flags_;
}

Status Request::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(w.WriteByte(0));
  TF_RETURN_IF_ERROR(w.skipToPosW(8));
  TF_RETURN_IF_ERROR(w.WriteInt(commandId()));
  TF_RETURN_IF_ERROR(w.skipToPosW(24));

  return Status::OK();
}

Response::Response() {

}

Status Response::read(IGFSClient &r) {
  //read Header
  TF_RETURN_IF_ERROR(r.Ignore(1));
  TF_RETURN_IF_ERROR(r.skipToPos(8));
  TF_RETURN_IF_ERROR(r.ReadInt(&requestId));
  TF_RETURN_IF_ERROR(r.skipToPos(24));
  TF_RETURN_IF_ERROR(r.ReadInt(&resType));
  bool hasError;
  TF_RETURN_IF_ERROR(r.ReadBool(hasError));

  if (hasError) {
    TF_RETURN_IF_ERROR(r.ReadString(error));
    TF_RETURN_IF_ERROR(r.ReadInt(&errorCode));
  } else {
    TF_RETURN_IF_ERROR(r.skipToPos(HEADER_SIZE + 6 - 1));
    TF_RETURN_IF_ERROR(r.ReadInt(&len));
    TF_RETURN_IF_ERROR(r.skipToPos(HEADER_SIZE + RESPONSE_HEADER_SIZE));
  }

  return Status::OK();
}

int Response::getResType() {
  return resType;
}

int Response::getRequestId() {
  return requestId;
}

bool Response::isOk() {
  return errorCode == -1;
}

string Response::getError() {
  return error;
}

int Response::getErrorCode() {
  return errorCode;
}


PathControlRequest::PathControlRequest(string userName, string path, string destPath, bool flag, bool collocate,
                                       map<string, string> props) :
    userName(std::move(userName)), path(std::move(path)), destPath(std::move(destPath)), flag(flag),
    collocate(collocate), props(
    std::move(props)) {
}

Status PathControlRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(Request::write(w));

  TF_RETURN_IF_ERROR(w.writeString(userName));
  TF_RETURN_IF_ERROR(writePath(w, path));
  TF_RETURN_IF_ERROR(writePath(w, destPath));
  TF_RETURN_IF_ERROR(w.writeBoolean(flag));
  TF_RETURN_IF_ERROR(w.writeBoolean(collocate));
  TF_RETURN_IF_ERROR(w.writeStringMap(props));

  return Status::OK();
}

Status PathControlRequest::writePath(IGFSClient &w, string &path) {
  TF_RETURN_IF_ERROR(w.writeBoolean(!path.empty()));
  if (!path.empty())
    TF_RETURN_IF_ERROR(w.writeString(path));

  return Status::OK();
}

Status StreamControlRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(w.WriteByte(0));
  TF_RETURN_IF_ERROR(w.skipToPosW(8));
  TF_RETURN_IF_ERROR(w.WriteInt(commandId()));
  TF_RETURN_IF_ERROR(w.WriteLong(streamId));
  TF_RETURN_IF_ERROR(w.WriteInt(len));

  return Status::OK();
}

StreamControlRequest::StreamControlRequest(long streamId, int len) :
    streamId(streamId),
    len(len) {
}

DeleteRequest::DeleteRequest(const string &path, bool flag) : PathControlRequest("", path, "", flag, false,
                                                                                 map<string, string>()) {}

int DeleteRequest::commandId() {
  return 7;
}

Status DeleteResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadBool(done));

  return Status::OK();
}

DeleteResponse::DeleteResponse() {

}

bool DeleteResponse::exists() {
  return done;
}

ExistsRequest::ExistsRequest(const std::string &path) : PathControlRequest(
    "", path,
    "", false,
    false,
    map<string, string>()) {}

int ExistsRequest::commandId() {
  return 2;
}

Status ExistsResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadBool(ex));

  return Status::OK();
}

ExistsResponse::ExistsResponse() {

}

bool ExistsResponse::exists() {
  return ex;
}

HandshakeRequest::HandshakeRequest(string fsName, string logDir) : fsName(std::move(fsName)),
                                                                   logDir(std::move(logDir)) {};

int HandshakeRequest::commandId() {
  return 0;
};

Status HandshakeRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(Request::write(w));

  TF_RETURN_IF_ERROR(w.writeString(fsName));
  TF_RETURN_IF_ERROR(w.writeString(logDir));

  return Status::OK();
}

HandshakeResponse::HandshakeResponse() = default;

Status HandshakeResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadNullableString(fsName));
  TF_RETURN_IF_ERROR(r.ReadLong(&blockSize));

  bool hasSampling;
  TF_RETURN_IF_ERROR(r.ReadBool(hasSampling));

  if (hasSampling) {
    TF_RETURN_IF_ERROR(r.ReadBool(sampling));
  }

  return Status::OK();
}

string HandshakeResponse::getFSName() {
  return fsName;
}

ListRequest::ListRequest(const string &path) : PathControlRequest("", path, "", false, false, map<string, string>()) {};

ListFilesRequest::ListFilesRequest(const string &path) : ListRequest(path) {}

int ListFilesRequest::commandId() {
  return 10;
}

int ListPathsRequest::commandId() {
  return 9;
}


OpenCreateRequest::OpenCreateRequest(const string &path) : PathControlRequest("", path, "", false, false,
                                                                              map<string, string>()) {}

Status OpenCreateRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(PathControlRequest::write(w));

  TF_RETURN_IF_ERROR(w.WriteInt(replication));
  TF_RETURN_IF_ERROR(w.WriteLong(blockSize));

  return Status::OK();
}

int OpenCreateRequest::commandId() {
  return 15;
}

Status OpenCreateResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadLong(&streamId));
}

long OpenCreateResponse::getStreamId() {
  return streamId;
}

OpenAppendRequest::OpenAppendRequest(const string &userName, const string &path) :
    PathControlRequest(userName, path, "", false, true, map<string, string>()) {}

Status OpenAppendRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(PathControlRequest::write(w));

  return Status::OK();
}

int OpenAppendRequest::commandId() {
  return 14;
}

Status OpenAppendResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadLong(&streamId));

  return Status::OK();
}

long OpenAppendResponse::getStreamId() {
  return streamId;
}

OpenReadRequest::OpenReadRequest(const string &userName, const string &path, bool flag, int seqReadsBeforePrefetch)
    : seqReadsBeforePrefetch(seqReadsBeforePrefetch), PathControlRequest(userName,
                                                                         path,
                                                                         "",
                                                                         flag,
                                                                         true,
                                                                         map<string, string>()) {}

OpenReadRequest::OpenReadRequest(const string &userName, const string &path) : OpenReadRequest(userName,
                                                                                               path,
                                                                                               false,
                                                                                               0) {}

Status OpenReadRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(PathControlRequest::write(w));

  if (flag) {
    TF_RETURN_IF_ERROR(w.WriteInt(seqReadsBeforePrefetch));
  }

  return Status::OK();
}

int OpenReadRequest::commandId() {
  return 13;
}

Status OpenReadResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadLong(&streamId));
  TF_RETURN_IF_ERROR(r.ReadLong(&len));

  return Status::OK();
}

OpenReadResponse::OpenReadResponse() = default;

long OpenReadResponse::getStreamId() {
  return streamId;
}

long OpenReadResponse::getLength() {
  return len;
}

InfoRequest::InfoRequest(const string &userName, const string &path) : PathControlRequest(userName, path, "", false,
                                                                                          false,
                                                                                          map<string, string>()) {}

int InfoRequest::commandId() {
  return 3;
}

Status InfoResponse::read(IGFSClient &r) {
  fileInfo = IgfsFile();
  TF_RETURN_IF_ERROR(fileInfo.read(r));

  return Status::OK();
}

IgfsFile InfoResponse::getFileInfo() {
  return fileInfo;
}

MakeDirectoriesRequest::MakeDirectoriesRequest(const string &userName, const string &path) : PathControlRequest(
    userName, path, "", false, false,
    map<string, string>()) {}

int MakeDirectoriesRequest::commandId() {
  return 8;
}

Status MakeDirectoriesResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadBool(succ));

  return Status::OK();
}

MakeDirectoriesResponse::MakeDirectoriesResponse() = default;

bool MakeDirectoriesResponse::successful() {
  return succ;
}

CloseRequest::CloseRequest(long streamId) : StreamControlRequest(streamId, 0) {}

int CloseRequest::commandId() {
  return 16;
}

CloseResponse::CloseResponse() : successful(false) {

}

Status CloseResponse::read(IGFSClient &r) {
  r.ReadBool(successful);
}

bool CloseResponse::isSuccessful() {
  return successful;
}

ReadBlockRequest::ReadBlockRequest(long streamId, long pos, int len) : StreamControlRequest(streamId, len), pos(pos) {}

Status ReadBlockRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(StreamControlRequest::write(w));

  TF_RETURN_IF_ERROR(w.WriteLong(pos));

  return Status::OK();
}

int ReadBlockRequest::commandId() {
  return 17;
}

Status ReadBlockResponse::read(IGFSClient &r, int length, char *dst) {
  TF_RETURN_IF_ERROR(r.ReadData(reinterpret_cast<uint8_t *>(dst), length));
  successfulyRead = length;

  return Status::OK();
}

Status ReadBlockResponse::read(IGFSClient &r) {
  // No-op
}

streamsize ReadBlockResponse::getSuccessfulyRead() {
  return successfulyRead;
}

ReadBlockControlResponse::ReadBlockControlResponse(char *dst) : dst(dst) {}

Status ReadBlockControlResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(Response::read(r));

  if (isOk()) {
    res = ReadBlockResponse();
    TF_RETURN_IF_ERROR(res.read(r, len, dst));
  }

  return Status::OK();
}

int ReadBlockControlResponse::getLength() {
  return len;
}

WriteBlockRequest::WriteBlockRequest(long streamId, const char *data, int len) :
    StreamControlRequest(streamId, len), data(data) {}

Status WriteBlockRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(StreamControlRequest::write(w));
  TF_RETURN_IF_ERROR(w.WriteData((uint8_t* )data, len));

  return Status::OK();
}

int WriteBlockRequest::commandId() {
  return 18;
}

RenameRequest::RenameRequest(const std::string &path, const std::string &destPath) : PathControlRequest("", path,
                                                                                                        destPath, false,
                                                                                                        false,
                                                                                                        std::map<std::string, std::string>()) {}

int RenameRequest::commandId() {
  return 6;
}

RenameResponse::RenameResponse() = default;

Status RenameResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadBool(ex));

  return Status::OK();
}

bool RenameResponse::successful() {
  return ex;
}




