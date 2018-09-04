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

#include "igfs_messages.h"

namespace tensorflow {

Status IgfsFile::Read(ExtendedTCPClient *r) {
  path_ = Optional<IgnitePath>();
  TF_RETURN_IF_ERROR(path_.Read(r));
  properties_ = map<string, string>();

  TF_RETURN_IF_ERROR(r->ReadInt(&block_size_));
  TF_RETURN_IF_ERROR(r->ReadLong(&group_block_size_));
  TF_RETURN_IF_ERROR(r->ReadLong(&length_));
  TF_RETURN_IF_ERROR(r->ReadStringMap(properties_));
  TF_RETURN_IF_ERROR(r->ReadLong(&access_time_));
  TF_RETURN_IF_ERROR(r->ReadLong(&modification_time_));
  TF_RETURN_IF_ERROR(r->ReadByte(&flags_));

  return Status::OK();
}

int64_t IgfsFile::GetFileSize() { return length_; }

int64_t IgfsFile::GetModificationTime() { return modification_time_; }

uint8_t IgfsFile::GetFlags() { return flags_; }

Status Request::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(w->WriteByte(0));
  TF_RETURN_IF_ERROR(w->FillWithZerosUntil(8));
  TF_RETURN_IF_ERROR(w->WriteInt(CommandId()));
  TF_RETURN_IF_ERROR(w->FillWithZerosUntil(24));

  return Status::OK();
}

Status Response::Read(ExtendedTCPClient *r) {
  // read Header
  TF_RETURN_IF_ERROR(r->Ignore(1));
  TF_RETURN_IF_ERROR(r->SkipToPos(8));
  TF_RETURN_IF_ERROR(r->ReadInt(&request_id));
  TF_RETURN_IF_ERROR(r->SkipToPos(24));
  TF_RETURN_IF_ERROR(r->ReadInt(&result_type));
  bool has_error;
  TF_RETURN_IF_ERROR(r->ReadBool(has_error));

  if (has_error) {
    TF_RETURN_IF_ERROR(r->ReadString(error));
    TF_RETURN_IF_ERROR(r->ReadInt(&error_code));
  } else {
    TF_RETURN_IF_ERROR(r->SkipToPos(HEADER_SIZE + 6 - 1));
    TF_RETURN_IF_ERROR(r->ReadInt(&length_));
    TF_RETURN_IF_ERROR(r->SkipToPos(HEADER_SIZE + RESPONSE_HEADER_SIZE));
  }

  return Status::OK();
}

int32_t Response::GetResType() { return result_type; }

int32_t Response::GetRequestId() { return request_id; }

bool Response::IsOk() { return error_code == -1; }

string Response::GetError() { return error; }

int32_t Response::GetErrorCode() { return error_code; }

PathControlRequest::PathControlRequest(string user_name, string path,
                                       string destination_path, bool flag,
                                       bool collocate,
                                       map<string, string> properties)
    : user_name_(std::move(user_name)),
      path_(std::move(path)),
      destination_path_(std::move(destination_path)),
      flag_(flag),
      collocate_(collocate),
      props_(std::move(properties)) {}

Status PathControlRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(Request::Write(w));

  TF_RETURN_IF_ERROR(w->WriteString(user_name_));
  TF_RETURN_IF_ERROR(WritePath(w, path_));
  TF_RETURN_IF_ERROR(WritePath(w, destination_path_));
  TF_RETURN_IF_ERROR(w->WriteBool(flag_));
  TF_RETURN_IF_ERROR(w->WriteBool(collocate_));
  TF_RETURN_IF_ERROR(w->WriteStringMap(props_));

  return Status::OK();
}

Status PathControlRequest::WritePath(ExtendedTCPClient *w, const string &path) {
  TF_RETURN_IF_ERROR(w->WriteBool(!path.empty()));
  if (!path.empty()) TF_RETURN_IF_ERROR(w->WriteString(path));

  return Status::OK();
}

Status StreamControlRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(w->WriteByte(0));
  TF_RETURN_IF_ERROR(w->FillWithZerosUntil(8));
  TF_RETURN_IF_ERROR(w->WriteInt(CommandId()));
  TF_RETURN_IF_ERROR(w->WriteLong(stream_id_));
  TF_RETURN_IF_ERROR(w->WriteInt(length_));

  return Status::OK();
}

StreamControlRequest::StreamControlRequest(int64_t stream_id, int32_t length)
    : stream_id_(stream_id), length_(length) {}

DeleteRequest::DeleteRequest(const string &path, bool flag)
    : PathControlRequest("", path, "", flag, true, map<string, string>()) {}

Status DeleteResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadBool(done));

  return Status::OK();
}

bool DeleteResponse::exists() { return done; }

ExistsRequest::ExistsRequest(const string &path)
    : PathControlRequest("", path, "", false, true, map<string, string>()) {}

Status ExistsResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadBool(ex));

  return Status::OK();
}

bool ExistsResponse::Exists() { return ex; }

HandshakeRequest::HandshakeRequest(const string &fs_name, const string &log_dir)
    : fs_name_(fs_name), log_dir_(log_dir){};

Status HandshakeRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(Request::Write(w));

  TF_RETURN_IF_ERROR(w->WriteString(fs_name_));
  TF_RETURN_IF_ERROR(w->WriteString(log_dir_));

  return Status::OK();
}

Status HandshakeResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadNullableString(fs_name_));
  TF_RETURN_IF_ERROR(r->ReadLong(&block_size_));

  bool has_sampling_;
  TF_RETURN_IF_ERROR(r->ReadBool(has_sampling_));

  if (has_sampling_) {
    TF_RETURN_IF_ERROR(r->ReadBool(sampling_));
  }

  return Status::OK();
}

string HandshakeResponse::GetFSName() { return fs_name_; }

ListRequest::ListRequest(const string &path)
    : PathControlRequest("", path, "", false, true, map<string, string>()){};

ListFilesRequest::ListFilesRequest(const string &path) : ListRequest(path) {}

OpenCreateRequest::OpenCreateRequest(const string &path)
    : PathControlRequest("", path, "", false, true, map<string, string>()) {}

Status OpenCreateRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(PathControlRequest::Write(w));

  TF_RETURN_IF_ERROR(w->WriteInt(replication_));
  TF_RETURN_IF_ERROR(w->WriteLong(blockSize_));

  return Status::OK();
}

Status OpenCreateResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadLong(&stream_id_));

  return Status::OK();
}

int64_t OpenCreateResponse::GetStreamId() { return stream_id_; }

OpenAppendRequest::OpenAppendRequest(const string &user_name,
                                     const string &path)
    : PathControlRequest(user_name, path, "", false, true,
                         map<string, string>()) {}

Status OpenAppendRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(PathControlRequest::Write(w));

  return Status::OK();
}

Status OpenAppendResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadLong(&stream_id_));

  return Status::OK();
}

int64_t OpenAppendResponse::GetStreamId() { return stream_id_; }

OpenReadRequest::OpenReadRequest(const string &user_name, const string &path,
                                 bool flag, int32_t sequential_reads_before_prefetch)
    : PathControlRequest(user_name, path, "", flag, true, map<string, string>()),
      sequential_reads_before_prefetch_(sequential_reads_before_prefetch) {}

OpenReadRequest::OpenReadRequest(const string &user_name, const string &path)
    : OpenReadRequest(user_name, path, false, 0) {}

Status OpenReadRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(PathControlRequest::Write(w));

  if (flag_) {
    TF_RETURN_IF_ERROR(w->WriteInt(sequential_reads_before_prefetch_));
  }

  return Status::OK();
}

Status OpenReadResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadLong(&stream_id_));
  TF_RETURN_IF_ERROR(r->ReadLong(&length_));

  return Status::OK();
}

int64_t OpenReadResponse::GetStreamId() { return stream_id_; }

int64_t OpenReadResponse::GetLength() { return length_; }

InfoRequest::InfoRequest(const string &userName, const string &path)
    : PathControlRequest(userName, path, "", false, true,
                         map<string, string>()) {}

Status InfoResponse::Read(ExtendedTCPClient *r) {
  fileInfo = IgfsFile();
  TF_RETURN_IF_ERROR(fileInfo.Read(r));

  return Status::OK();
}

IgfsFile InfoResponse::getFileInfo() { return fileInfo; }

MakeDirectoriesRequest::MakeDirectoriesRequest(const string &userName,
                                               const string &path)
    : PathControlRequest(userName, path, "", false, true,
                         map<string, string>()) {}

Status MakeDirectoriesResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadBool(successful_));

  return Status::OK();
}

MakeDirectoriesResponse::MakeDirectoriesResponse() = default;

bool MakeDirectoriesResponse::IsSuccessful() { return successful_; }

CloseRequest::CloseRequest(int64_t streamId)
    : StreamControlRequest(streamId, 0) {}

Status CloseResponse::Read(ExtendedTCPClient *r) { 
  TF_RETURN_IF_ERROR(r->ReadBool(successful_));

  return Status::OK(); 
}

bool CloseResponse::IsSuccessful() { return successful_; }

ReadBlockRequest::ReadBlockRequest(int64_t stream_id, int64_t pos,
                                   int32_t length)
    : StreamControlRequest(stream_id, length), pos(pos) {}

Status ReadBlockRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(StreamControlRequest::Write(w));

  TF_RETURN_IF_ERROR(w->WriteLong(pos));

  return Status::OK();
}

Status ReadBlockResponse::Read(ExtendedTCPClient *r, int32_t length,
                               uint8_t *dst) {
  TF_RETURN_IF_ERROR(r->ReadData(dst, length));
  successfuly_read = length;

  return Status::OK();
}

Status ReadBlockResponse::Read(ExtendedTCPClient *r) {
  return Status::OK();
}

streamsize ReadBlockResponse::GetSuccessfulyRead() { return successfuly_read; }

ReadBlockControlResponse::ReadBlockControlResponse(uint8_t *dst) : dst(dst) {}

Status ReadBlockControlResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(Response::Read(r));

  if (IsOk()) {
    res = ReadBlockResponse();
    TF_RETURN_IF_ERROR(res.Read(r, length_, dst));
  }

  return Status::OK();
}

int32_t ReadBlockControlResponse::GetLength() { return length_; }

WriteBlockRequest::WriteBlockRequest(int64_t stream_id, const uint8_t *data,
                                     int32_t length)
    : StreamControlRequest(stream_id, length), data(data) {}

Status WriteBlockRequest::Write(ExtendedTCPClient *w) {
  TF_RETURN_IF_ERROR(StreamControlRequest::Write(w));
  TF_RETURN_IF_ERROR(w->WriteData((uint8_t *)data, length_));

  return Status::OK();
}

RenameRequest::RenameRequest(const string &path, const string &destination_path)
    : PathControlRequest("", path, destination_path, false, true,
                         map<string, string>()) {}

Status RenameResponse::Read(ExtendedTCPClient *r) {
  TF_RETURN_IF_ERROR(r->ReadBool(ex));

  return Status::OK();
}

bool RenameResponse::IsSuccessful() { return ex; }

}  // namespace tensorflow