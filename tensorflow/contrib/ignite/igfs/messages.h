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

#ifndef TF_IGFS_MESSAGES_H
#define TF_IGFS_MESSAGES_H

#include <string>
#include <map>
#include <vector>
#include "utils.h"

namespace tensorflow {

using std::map;
using std::string;
using std::vector;
using std::streamsize;

class IgnitePath {
 public:
  Status Read(IGFSClient &r) {
    return r.ReadNullableString(path_);
  }

  string getPath() {
    return path_;
  }

 private:
  string path_;
};

class IgfsFile {
 public:
  IgfsFile() = default;

  Status Read(IGFSClient &r);

  long GetFileSize();

  long GetModificationTime();

  char GetFlags();

 private:
  Optional<IgnitePath> path_;
  int block_size_{};
  long group_block_size_{};
  long length_{};
  map<string, string> properties_;
  long access_time_{};
  long modification_time_{};
  char flags_{};
};

class Request {
 public:
  virtual int CommandId() = 0;

  virtual Status write(IGFSClient &w);
};

class Response {
 public:
  Response();

  virtual Status Read(IGFSClient &r);

  int getResType();

  int getRequestId();

  bool isOk();

  string getError();

  int getErrorCode();

 protected:
  string error;
  int request_id;
  int length_;
  int result_type;
  int error_code = -1;
  static const int HEADER_SIZE = 24;
  static const int RESPONSE_HEADER_SIZE = 9;
  static const int RES_TYPE_ERR_STREAM_ID = 9;
};

class PathControlRequest : public Request {
 public:
  PathControlRequest(string user_name,
                     string path,
                     string destination_path,
                     bool flag,
                     bool collocate,
                     map<string, string> properties);

  Status write(IGFSClient &w);

 protected:
  /** Main path. */
  string path;

  /** Second path, rename command. */
  string destination_path;

  /** Boolean flag, meaning depends on command. */
  bool flag;

  /** Boolean flag controlling whether file will be colocated on single node. */
  bool collocate;

  /** Properties. */
  map<string, string> props;

  /** The user name this control request is made on behalf of. */
  string user_name_;

  Status writePath(IGFSClient &w, string &path);
};

class StreamControlRequest : public Request {
 public:
  StreamControlRequest(long stream_id, int length);

  Status write(IGFSClient &w) override;

 protected:
  long stream_id_;

  int length_;
};

template<class R>
class ControlResponse : public Response {
 public:
  ControlResponse() = default;

  Status Read(IGFSClient &r) override {
    TF_RETURN_IF_ERROR(Response::Read(r));

    if (isOk()) {
      res = R();
      TF_RETURN_IF_ERROR(res.Read(r));
    }

    return Status::OK();
  }

  R getRes() {
    return res;
  }

 protected:
  R res;
};

class DeleteRequest : public PathControlRequest {
 public:
  DeleteRequest(const string &path, bool flag);

  inline int CommandId() override {
    return 7;
  }
};

class DeleteResponse {
 public:
  Status Read(IGFSClient &r);

  DeleteResponse();

  bool exists();

 private:
  bool done;
};

class ExistsRequest : public PathControlRequest {
 public:
  explicit ExistsRequest(const string &userName);

  inline int CommandId() override {
    return 2;
  }
};

class ExistsResponse {
 public:
  Status Read(IGFSClient &r);

  ExistsResponse();

  bool exists();

 private:
  bool ex;
};

class HandshakeRequest : Request {
 public:
  HandshakeRequest(string fs_name, string log_dir);

  inline int CommandId() override {
    return 0;
  }

  Status write(IGFSClient &w) override;

 private:
  string fs_name_;
  string log_dir_;
};

class HandshakeResponse {
 public:
  HandshakeResponse();

  Status Read(IGFSClient &r);

  string getFSName();

 private:
  string fs_name_;
  long block_size_{};
  bool sampling_{};
};

class ListRequest : public PathControlRequest {
 public:
  explicit ListRequest(const string &path);
};

template<class T>
class ListResponse {
 public:
  Status Read(IGFSClient &r) {
    int len;
    TF_RETURN_IF_ERROR(r.ReadInt(&len));

    entries = vector<T>();

    for (int i = 0; i < len; i++) {
      T f = {};
      TF_RETURN_IF_ERROR(f.Read(r));
      entries.push_back(f);
    }

    return Status::OK();
  }

  vector<T> getEntries() {
    return entries;
  }

 protected:
  vector<T> entries;
};

class ListFilesRequest : public ListRequest {
 public:
  explicit ListFilesRequest(const string &path);

  inline int CommandId() override {
    return 10;
  }
};

class ListFilesResponse : public ListResponse<IgfsFile> {

};

class ListPathsRequest : public ListRequest {
 public:
  explicit ListPathsRequest(const string &path) : ListRequest(path) {}

  inline int CommandId() override {
    return 9;
  }
};

class ListPathsResponse : public ListResponse<IgnitePath> {

};

class OpenCreateRequest : PathControlRequest {
 public:
  explicit OpenCreateRequest(const string &path);

  Status write(IGFSClient &w) override;

  inline int CommandId() override {
    return 15;
  }

 protected:
  /** Replication factor. */
  int replication_{};

  /** Block size. */
  long blockSize_{};
};

class OpenCreateResponse {
 public:
  OpenCreateResponse() = default;

  Status Read(IGFSClient &r);

  long getStreamId();

 private:
  long stream_id_{};
};

class OpenAppendRequest : PathControlRequest {
 public:
  explicit OpenAppendRequest(const string& user_name, const string& path);

  Status write(IGFSClient &w) override;

  inline int CommandId() override {
    return 14;
  }
};

class OpenAppendResponse {
 public:
  OpenAppendResponse() = default;

  Status Read(IGFSClient &r);

  long getStreamId();

 private:
  long stream_id_{};
};

class OpenReadRequest : PathControlRequest {
 public:
  OpenReadRequest(const string &user_name,
                  const string &path,
                  bool flag,
                  int seqReadsBeforePrefetch);

  OpenReadRequest(const string &userName, const string &path);

  Status write(IGFSClient &w) override;

  inline int CommandId() override {
    return 13;
  }

 protected:
  /** Sequential reads before prefetch. */
  int sequential_reads_before_prefetch;
};

class OpenReadResponse {
 public:
  OpenReadResponse();

  Status Read(IGFSClient &r);

  long getStreamId();

  long getLength();

 private:
  long stream_id_{};
  long length_{};
};

class InfoRequest : public PathControlRequest {
 public:
  InfoRequest(const string& userName, const string &path);

  inline int CommandId() override {
    return 3;
  }
};

class InfoResponse {
 public:
  InfoResponse() = default;

  Status Read(IGFSClient &r);

  IgfsFile getFileInfo();

 private:
  IgfsFile fileInfo;
};

class MakeDirectoriesRequest : public PathControlRequest {
 public:
  MakeDirectoriesRequest(const string& userName, const string &path);

  inline int CommandId() override {
    return 8;
  }
};

class MakeDirectoriesResponse {
 public:
  MakeDirectoriesResponse();

  Status Read(IGFSClient &r);

  bool successful();

 private:
  bool successful_{};
};

/** Stream control requests. **/

class CloseRequest : public StreamControlRequest {
 public:
  explicit CloseRequest(long streamId);

  inline int CommandId() override {
    return 16;
  }
};

class CloseResponse {
 public:
  CloseResponse();

  Status Read(IGFSClient &r);

  bool isSuccessful();

 private:
  bool successful_;
};

class ReadBlockRequest : public StreamControlRequest {
 public:
  ReadBlockRequest(long stream_id, long pos, int length);

  Status write(IGFSClient &w) override;

  inline int CommandId() override {
    return 17;
  }

 private:
  long pos;
};

class ReadBlockResponse {
 public:
  Status Read(IGFSClient &r, int length, char *dst);

  Status Read(IGFSClient &r);

  streamsize getSuccessfulyRead();

 private:
  int length;
  streamsize successfulyRead;
};

class ReadBlockControlResponse : public ControlResponse<ReadBlockResponse> {
 public:
  explicit ReadBlockControlResponse(char *dst);

  Status Read(IGFSClient &r) override;

  int getLength();

 private:
  char *dst;
};

class WriteBlockRequest : public StreamControlRequest {
 public:
  WriteBlockRequest(long stream_id, const char *data, int length);

  Status write(IGFSClient &w) override;

  inline int CommandId() override {
    return 18;
  }

 private:
  const char *data;
};

class RenameRequest : public PathControlRequest {
 public:
  RenameRequest(const std::string &path, const std::string &destination_path);

  inline int CommandId() override {
    return 6;
  }
};

class RenameResponse {
 public:
  RenameResponse();

  Status Read(IGFSClient &r);

  bool IsSuccessful();

 private:
  bool ex{};
};

}

#endif
