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

Status IgfsFile::read(IGFSClient &r) {
  path_ = Optional<IgnitePath>();
  TF_RETURN_IF_ERROR(path_.read(r));
  properties_ = map<string, string>();

  TF_RETURN_IF_ERROR(r.ReadInt(&block_size_));
  TF_RETURN_IF_ERROR(r.ReadLong(&group_block_size_));
  TF_RETURN_IF_ERROR(r.ReadLong(&length_));
  TF_RETURN_IF_ERROR(r.ReadStringMap(properties_));
  TF_RETURN_IF_ERROR(r.ReadLong(&access_time_));
  TF_RETURN_IF_ERROR(r.ReadLong(&modification_time_));
  TF_RETURN_IF_ERROR(r.ReadByte((uint8_t *) &flags_));

  return Status::OK();
}

long IgfsFile::getFileSize() {
  return length_;
}

long IgfsFile::getModificationTime() {
  return modification_time_;
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
  TF_RETURN_IF_ERROR(r.ReadInt(&request_id));
  TF_RETURN_IF_ERROR(r.skipToPos(24));
  TF_RETURN_IF_ERROR(r.ReadInt(&result_type));
  bool has_error;
  TF_RETURN_IF_ERROR(r.ReadBool(has_error));

  if (has_error) {
    TF_RETURN_IF_ERROR(r.ReadString(error));
    TF_RETURN_IF_ERROR(r.ReadInt(&error_code));
  } else {
    TF_RETURN_IF_ERROR(r.skipToPos(HEADER_SIZE + 6 - 1));
    TF_RETURN_IF_ERROR(r.ReadInt(&length_));
    TF_RETURN_IF_ERROR(r.skipToPos(HEADER_SIZE + RESPONSE_HEADER_SIZE));
  }

  return Status::OK();
}

int Response::getResType() {
  return result_type;
}

int Response::getRequestId() {
  return request_id;
}

bool Response::isOk() {
  return error_code == -1;
}

string Response::getError() {
  return error;
}

int Response::getErrorCode() {
  return error_code;
}


PathControlRequest::PathControlRequest(string user_name,
                                       string path,
                                       string destination_path,
                                       bool flag,
                                       bool collocate,
                                       map<string, string> properties) :
    user_name_(std::move(user_name)),
    path(std::move(path)),
    destination_path(std::move(destination_path)),
    flag(flag),
    collocate(collocate),
    props(std::move(properties)) {
}

Status PathControlRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(Request::write(w));

  TF_RETURN_IF_ERROR(w.writeString(user_name_));
  TF_RETURN_IF_ERROR(writePath(w, path));
  TF_RETURN_IF_ERROR(writePath(w, destination_path));
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
  TF_RETURN_IF_ERROR(w.WriteLong(stream_id_));
  TF_RETURN_IF_ERROR(w.WriteInt(length_));

  return Status::OK();
}

StreamControlRequest::StreamControlRequest(long stream_id, int length) :
    stream_id_(stream_id),
    length_(length) {
}

DeleteRequest::DeleteRequest(const string &path,
                             bool flag)
    : PathControlRequest("",
                         path,
                         "",
                         flag,
                         false,
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

ExistsRequest::ExistsRequest(const string &path) : PathControlRequest(
    "",
    path,
    "",
    false,
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

HandshakeRequest::HandshakeRequest(string fs_name, string log_dir) :
    fs_name_(std::move(fs_name)),
    log_dir_(std::move(log_dir)) {};

int HandshakeRequest::commandId() {
  return 0;
};

Status HandshakeRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(Request::write(w));

  TF_RETURN_IF_ERROR(w.writeString(fs_name_));
  TF_RETURN_IF_ERROR(w.writeString(log_dir_));

  return Status::OK();
}

HandshakeResponse::HandshakeResponse() = default;

Status HandshakeResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadNullableString(fs_name_));
  TF_RETURN_IF_ERROR(r.ReadLong(&block_size_));

  bool has_sampling_;
  TF_RETURN_IF_ERROR(r.ReadBool(has_sampling_));

  if (has_sampling_) {
    TF_RETURN_IF_ERROR(r.ReadBool(sampling_));
  }

  return Status::OK();
}

string HandshakeResponse::getFSName() {
  return fs_name_;
}

ListRequest::ListRequest(const string &path) : PathControlRequest(
    "",
    path,
    "",
    false,
    false,
    map<string, string>()) {};

ListFilesRequest::ListFilesRequest(const string &path) : ListRequest(path) {}

int ListFilesRequest::commandId() {
  return 10;
}

int ListPathsRequest::commandId() {
  return 9;
}

OpenCreateRequest::OpenCreateRequest(const string &path) : PathControlRequest(
    "",
    path,
    "",
    false,
    false,
    map<string, string>()) {}

Status OpenCreateRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(PathControlRequest::write(w));

  TF_RETURN_IF_ERROR(w.WriteInt(replication_));
  TF_RETURN_IF_ERROR(w.WriteLong(blockSize_));

  return Status::OK();
}

int OpenCreateRequest::commandId() {
  return 15;
}

Status OpenCreateResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadLong(&stream_id_));
}

long OpenCreateResponse::getStreamId() {
  return stream_id_;
}

OpenAppendRequest::OpenAppendRequest(const string &user_name,
                                     const string &path) :
    PathControlRequest(user_name,
                       path,
                       "",
                       false,
                       true,
                       map<string, string>()) {}

Status OpenAppendRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(PathControlRequest::write(w));

  return Status::OK();
}

int OpenAppendRequest::commandId() {
  return 14;
}

Status OpenAppendResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadLong(&stream_id_));

  return Status::OK();
}

long OpenAppendResponse::getStreamId() {
  return stream_id_;
}

OpenReadRequest::OpenReadRequest(const string &user_name,
                                 const string &path,
                                 bool flag,
                                 int seqReadsBeforePrefetch)
    : sequential_reads_before_prefetch(seqReadsBeforePrefetch),
      PathControlRequest(user_name,
                         path,
                         "",
                         flag,
                         true,
                         map<string, string>()) {}

OpenReadRequest::OpenReadRequest(const string &userName, const string &path)
    : OpenReadRequest(userName,
                      path,
                      false,
                      0) {}

Status OpenReadRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(PathControlRequest::write(w));

  if (flag) {
    TF_RETURN_IF_ERROR(w.WriteInt(sequential_reads_before_prefetch));
  }

  return Status::OK();
}

int OpenReadRequest::commandId() {
  return 13;
}

Status OpenReadResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadLong(&stream_id_));
  TF_RETURN_IF_ERROR(r.ReadLong(&length_));

  return Status::OK();
}

OpenReadResponse::OpenReadResponse() = default;

long OpenReadResponse::getStreamId() {
  return stream_id_;
}

long OpenReadResponse::getLength() {
  return length_;
}

InfoRequest::InfoRequest(const string &userName, const string &path)
    : PathControlRequest(userName, path, "", false,
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

MakeDirectoriesRequest::MakeDirectoriesRequest(const string &userName,
                                               const string &path)
    : PathControlRequest(
    userName, path, "", false, false,
    map<string, string>()) {}

int MakeDirectoriesRequest::commandId() {
  return 8;
}

Status MakeDirectoriesResponse::read(IGFSClient &r) {
  TF_RETURN_IF_ERROR(r.ReadBool(successful_));

  return Status::OK();
}

MakeDirectoriesResponse::MakeDirectoriesResponse() = default;

bool MakeDirectoriesResponse::successful() {
  return successful_;
}

CloseRequest::CloseRequest(long streamId) : StreamControlRequest(streamId, 0) {}

int CloseRequest::commandId() {
  return 16;
}

CloseResponse::CloseResponse() : successful_(false) {

}

Status CloseResponse::read(IGFSClient &r) {
  r.ReadBool(successful_);
}

bool CloseResponse::isSuccessful() {
  return successful_;
}

ReadBlockRequest::ReadBlockRequest(long stream_id, long pos, int length)
    : StreamControlRequest(stream_id, length), pos(pos) {}

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
    TF_RETURN_IF_ERROR(res.read(r, length_, dst));
  }

  return Status::OK();
}

int ReadBlockControlResponse::getLength() {
  return length_;
}

WriteBlockRequest::WriteBlockRequest(long stream_id,
                                     const char *data,
                                     int length) :
    StreamControlRequest(stream_id, length), data(data) {}

Status WriteBlockRequest::write(IGFSClient &w) {
  TF_RETURN_IF_ERROR(StreamControlRequest::write(w));
  TF_RETURN_IF_ERROR(w.WriteData((uint8_t *) data, length_));

  return Status::OK();
}

int WriteBlockRequest::commandId() {
  return 18;
}

RenameRequest::RenameRequest(const std::string &path,
                             const std::string &destination_path) :
    PathControlRequest(
    "",
    path,
    destination_path,
    false,
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