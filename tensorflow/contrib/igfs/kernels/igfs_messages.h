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

#ifndef TENSORFLOW_CONTRIB_IGFS_KERNELS_IGFS_MESSAGES_H_
#define TENSORFLOW_CONTRIB_IGFS_KERNELS_IGFS_MESSAGES_H_

#include <map>
#include <string>
#include <vector>

#include "igfs_utils.h"

namespace tensorflow {

using std::map;
using std::string;
using std::vector;
using std::streamsize;

enum CommandId {
  HANDSHAKE_ID = 0,
  EXISTS_ID = 2,
  INFO_ID = 3,
  RENAME_ID = 6,
  DELETE_ID = 7,
  MKDIR_ID = 8,
  LIST_PATHS_ID = 9,
  LIST_FILES_ID = 10,
  OPEN_READ_ID = 13,
  OPEN_APPEND_ID = 14,
  OPEN_CREATE_ID = 15,
  CLOSE_ID = 16,
  READ_BLOCK_ID = 17,
  WRITE_BLOCK_ID = 18,
};

class IGFSPath {
 public:
  std::string path;

  Status Read(ExtendedTCPClient *client);
};

class IGFSFile {
 public:
  int64_t length;
  int64_t modification_time;
  uint8_t flags;

  Status Read(ExtendedTCPClient *client);
};

class Request {
 public:
  Request(int32_t command_id);
  virtual Status Write(ExtendedTCPClient *client) const;

 protected:
  const int32_t command_id_;
};

class Response {
 public:
  int32_t res_type;
  int32_t req_id;

  virtual Status Read(ExtendedTCPClient *client);

 protected:
  int32_t length_;
  static const int32_t HEADER_SIZE = 24;
  static const int32_t RESPONSE_HEADER_SIZE = 9;
  static const int32_t RES_TYPE_ERR_STREAM_ID = 9;
};

class PathControlRequest : public Request {
 public:
  PathControlRequest(int32_t command_id, string user_name, string path, string destination_path,
                     bool flag, bool collocate, map<string, string> properties);

  Status Write(ExtendedTCPClient *client) const override;

 protected:
  /** The user name this control request is made on behalf of. */
  const string user_name_;

  /** Main path. */
  const string path_;

  /** Second path, rename command. */
  const string destination_path_;

  /** Boolean flag, meaning depends on command. */
  const bool flag_;

  /** Boolean flag controlling whether file will be colocated on single node. */
  const bool collocate_;

  /** Properties. */
  const map<string, string> props_;

  Status WritePath(ExtendedTCPClient *client, const string &path) const;
};

class StreamControlRequest : public Request {
 public:
  StreamControlRequest(int32_t command_id, int64_t stream_id, int32_t length);

  Status Write(ExtendedTCPClient *client) const override;

 protected:
  int64_t stream_id_;
  int32_t length_;
};

template <class R>
class CtrlResponse : public Response {
 public:
  Status Read(ExtendedTCPClient *client) override {
    TF_RETURN_IF_ERROR(Response::Read(client));

    res = R();
    TF_RETURN_IF_ERROR(res.Read(client));

    return Status::OK();
  }

  R GetRes() { return res; }

 protected:
  R res;
};

class DeleteRequest : public PathControlRequest {
 public:
  DeleteRequest(const string &user_name, const string &path, bool flag);
};

class DeleteResponse {
 public:
  bool exists;

  Status Read(ExtendedTCPClient *client);
};

class ExistsRequest : public PathControlRequest {
 public:
  explicit ExistsRequest(const string &user_name, const string &path);
};

class ExistsResponse {
 public:
  bool exists;

  Status Read(ExtendedTCPClient *client);
};

class HandshakeRequest : public Request {
 public:
  HandshakeRequest(const string &fs_name, const string &log_dir);
  Status Write(ExtendedTCPClient *client) const override;

 private:
  string fs_name_;
  string log_dir_;
};

class HandshakeResponse {
 public:
  string fs_name;
  Status Read(ExtendedTCPClient *client);
};

class ListRequest : public PathControlRequest {
 public:
  explicit ListRequest(int32_t command_id, const string &user_name, const string &path);
};

template <class T>
class ListResponse {
 public:
  vector<T> entries;
  Status Read(ExtendedTCPClient *client) {
    int32_t len;
    TF_RETURN_IF_ERROR(client->ReadInt(&len));
   
    LOG(INFO) << "List response length " << len;

    entries = vector<T>();

    for (int32_t i = 0; i < len; i++) {
      T f = {};
      TF_RETURN_IF_ERROR(f.Read(client));
      entries.push_back(f);
    }

    return Status::OK();
  }
};

class ListFilesRequest : public ListRequest {
 public:
  explicit ListFilesRequest(const string &user_name, const string &path);
};

class ListFilesResponse : public ListResponse<IGFSFile> {};

class ListPathsRequest : public ListRequest {
 public:
  ListPathsRequest(const string &user_name, const string &path);
};

class ListPathsResponse : public ListResponse<IGFSPath> {};

class OpenCreateRequest : public PathControlRequest {
 public:
  explicit OpenCreateRequest(const string &user_name, const string &path);

  Status Write(ExtendedTCPClient *client) const override;

 protected:
  /** Replication factor. */
  int32_t replication_{};

  /** Block size. */
  int64_t blockSize_{};
};

class OpenCreateResponse {
 public:
  int64_t stream_id;

  Status Read(ExtendedTCPClient *client);
};

class OpenAppendRequest : public PathControlRequest {
 public:
  explicit OpenAppendRequest(const string &user_name, const string &path);
  Status Write(ExtendedTCPClient *client) const override;
};

class OpenAppendResponse {
 public:
  int64_t stream_id;

  Status Read(ExtendedTCPClient *client);
};

class OpenReadRequest : public PathControlRequest {
 public:
  OpenReadRequest(const string &user_name, const string &path, bool flag,
                  int32_t seqReadsBeforePrefetch);

  OpenReadRequest(const string &user_name, const string &path);

  Status Write(ExtendedTCPClient *client) const override;

 protected:
  /** Sequential reads before prefetch. */
  int32_t sequential_reads_before_prefetch_;
};

class OpenReadResponse {
 public:
  int64_t stream_id;
  int64_t length;

  Status Read(ExtendedTCPClient *client);
};

class InfoRequest : public PathControlRequest {
 public:
  InfoRequest(const string &user_name, const string &path);
};

class InfoResponse {
 public:
  IGFSFile file_info;

  Status Read(ExtendedTCPClient *client);
};

class MakeDirectoriesRequest : public PathControlRequest {
 public:
  MakeDirectoriesRequest(const string &userName, const string &path);
};

class MakeDirectoriesResponse {
 public:
  bool successful;

  Status Read(ExtendedTCPClient *client);
};

/** Stream control requests. **/

class CloseRequest : public StreamControlRequest {
 public:
  explicit CloseRequest(int64_t stream_id);
};

class CloseResponse {
 public:
  Status Read(ExtendedTCPClient *client);

  bool IsSuccessful();

 private:
  bool successful_;
};

class ReadBlockRequest : public StreamControlRequest {
 public:
  ReadBlockRequest(int64_t stream_id, int64_t pos, int32_t length);

  Status Write(ExtendedTCPClient *client) const override;

 private:
  int64_t pos;
};

class ReadBlockResponse {
 public:
  Status Read(ExtendedTCPClient *client, int32_t length, uint8_t *dst);

  Status Read(ExtendedTCPClient *client);

  streamsize GetSuccessfulyRead();

 private:
  int32_t length;
  streamsize successfuly_read;
};

class ReadBlockCtrlResponse : public CtrlResponse<ReadBlockResponse> {
 public:
  explicit ReadBlockCtrlResponse(uint8_t *dst);

  Status Read(ExtendedTCPClient *client) override;

  int32_t GetLength();

 private:
  uint8_t *dst;
};

class WriteBlockRequest : public StreamControlRequest {
 public:
  WriteBlockRequest(int64_t stream_id, const uint8_t *data, int32_t length);

  Status Write(ExtendedTCPClient *client) const override;

 private:
  const uint8_t *data;
};

class RenameRequest : public PathControlRequest {
 public:
  RenameRequest(const std::string &user_name, const std::string &path, const std::string &destination_path);
};

class RenameResponse {
 public:
  Status Read(ExtendedTCPClient *client);

  bool IsSuccessful();

 private:
  bool ex{};
};

}  // namespace tensorflow

#endif
