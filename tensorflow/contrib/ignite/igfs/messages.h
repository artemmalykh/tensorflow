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
#include <utility>
#include <vector>
#include "utils.h"

using namespace std;
using namespace tensorflow;

namespace ignite {

class IgnitePath {
 public:
  Status read(IGFSClient &r) {
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

  Status read(IGFSClient &r);

  long getFileSize();

  long getModificationTime();

  char getFlags();

 private:
  Optional<IgnitePath> path_;
  int blockSize_{};
  long grpBlockSize_{};
  long len_{};
  map<string, string> props_;
  long accessTime_{};
  long modificationTime_{};
  char flags_{};
};

class Request {
 public:
  virtual int commandId() = 0;

  virtual Status write(IGFSClient &w);
};

class Response {
 public:
  Response();

  virtual Status read(IGFSClient &r);

  int getResType();

  int getRequestId();

  bool isOk();

  string getError();

  int getErrorCode();

 protected:
  string error;
  int requestId;
  int len;
  int resType;
  int errorCode = -1;
  static const int HEADER_SIZE = 24;
  static const int RESPONSE_HEADER_SIZE = 9;
  static const int RES_TYPE_ERR_STREAM_ID = 9;
};

class PathControlRequest : public Request {
 public:
  PathControlRequest(string userName, string path, string destPath, bool flag, bool collocate,
                     map<string, string> props);

  Status write(IGFSClient &w);

 protected:
  /** Main path. */
  string path;

  /** Second path, rename command. */
  string destPath;

  /** Boolean flag, meaning depends on command. */
  bool flag;

  /** Boolean flag which controls whether file will be colocated on single node. */
  bool collocate;

  /** Properties. */
  map<string, string> props;

  /** The user name this control request is made on behalf of. */
  string userName;

  Status writePath(IGFSClient &w, string &path);
};

class StreamControlRequest : public Request {
 public:
  StreamControlRequest(long streamId, int len);

  Status write(IGFSClient &w) override;

 protected:
  long streamId;

  int len;
};

template<class R>
class ControlResponse : public Response {
 public:
  ControlResponse() = default;

  Status read(IGFSClient &r) override {
    TF_RETURN_IF_ERROR(Response::read(r));

    if (isOk()) {
      res = R();
      TF_RETURN_IF_ERROR(res.read(r));
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

 private:
  int commandId() override;
};

class DeleteResponse {
 public:
  Status read(IGFSClient &r);

  DeleteResponse();

  bool exists();

 private:
  bool done;
};

class ExistsRequest : public PathControlRequest {
 public:
  explicit ExistsRequest(const string &userName);

  int commandId() override;
};

class ExistsResponse {
 public:
  Status read(IGFSClient &r);

  ExistsResponse();

  bool exists();

 private:
  bool ex;
};

class HandshakeRequest : Request {
 public:
  HandshakeRequest(string fsName, string logDir);

  int commandId() override;

  Status write(IGFSClient &w) override;

 private:
  string fsName;
  string logDir;
};

class HandshakeResponse {
 public:
  HandshakeResponse();

  Status read(IGFSClient &r);

  string getFSName();

 private:
  string fsName;
  long blockSize{};
  bool sampling{};
};

class ListRequest : public PathControlRequest {
 public:
  explicit ListRequest(const string &path);
};

template<class T>
class ListResponse {
 public:
  Status read(IGFSClient &r) {
    int len;
    TF_RETURN_IF_ERROR(r.ReadInt(len));

    entries = vector<T>();

    for (int i = 0; i < len; i++) {
      T f = {};
      TF_RETURN_IF_ERROR(f.read(r));
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

  int commandId() override;
};

class ListFilesResponse : public ListResponse<IgfsFile> {

};

class ListPathsRequest : public ListRequest {
 public:
  explicit ListPathsRequest(const string &path) : ListRequest(path) {}

  int commandId() override;
};

class ListPathsResponse : public ListResponse<IgnitePath> {

};

class OpenCreateRequest : PathControlRequest {
 public:
  explicit OpenCreateRequest(const string &path);

  Status write(IGFSClient &w) override;

  int commandId() override;

 protected:
  /** Hadoop replication factor. */
  int replication{};

  /** Hadoop block size. */
  long blockSize{};
};

class OpenCreateResponse {
 public:
  OpenCreateResponse() = default;

  Status read(IGFSClient &r);

  long getStreamId();

 private:
  long streamId{};
};

class OpenAppendRequest : PathControlRequest {
 public:
  explicit OpenAppendRequest(const string& userName, const string& path);

  Status write(IGFSClient &w) override;

  int commandId() override;
};

class OpenAppendResponse {
 public:
  OpenAppendResponse() = default;

  Status read(IGFSClient &r);

  long getStreamId();

 private:
  long streamId{};
};

class OpenReadRequest : PathControlRequest {
 public:
  OpenReadRequest(const string &userName, const string &path, bool flag, int seqReadsBeforePrefetch);

  OpenReadRequest(const string &userName, const string &path);

  Status write(IGFSClient &w) override;

  int commandId() override;

 protected:
  /** Sequential reads before prefetch. */
  int seqReadsBeforePrefetch;
};

class OpenReadResponse {
 public:
  OpenReadResponse();

  Status read(IGFSClient &r);

  long getStreamId();

  long getLength();

 private:
  long streamId{};
  long len{};
};

class InfoRequest : public PathControlRequest {
 public:
  InfoRequest(const string& userName, const string &path);

  int commandId() override;
};

class InfoResponse {
 public:
  InfoResponse() = default;

  Status read(IGFSClient &r);

  IgfsFile getFileInfo();

 private:
  IgfsFile fileInfo;
};

class MakeDirectoriesRequest : public PathControlRequest {
 public:
  MakeDirectoriesRequest(const string& userName, const string &path);

  int commandId() override;
};

class MakeDirectoriesResponse {
 public:
  MakeDirectoriesResponse();

  Status read(IGFSClient &r);

  bool successful();

 private:
  bool succ{};
};

/** Stream control requests. **/

class CloseRequest : public StreamControlRequest {
 public:
  explicit CloseRequest(long streamId);

  int commandId() override;
};

class CloseResponse {
 public:
  CloseResponse();

  Status read(IGFSClient &r);

  bool isSuccessful();

 private:
  bool successful;
};

class ReadBlockRequest : public StreamControlRequest {
 public:
  ReadBlockRequest(long streamId, long pos, int len);

  Status write(IGFSClient &w) override;

  int commandId() override;

 private:
  long pos;
};

class ReadBlockResponse {
 public:
  Status read(IGFSClient &r, int length, char *dst);

  Status read(IGFSClient &r);

  streamsize getSuccessfulyRead();

 private:
  int length;
  streamsize successfulyRead;
};

class ReadBlockControlResponse : public ControlResponse<ReadBlockResponse> {
 public:
  explicit ReadBlockControlResponse(char *dst);

  Status read(IGFSClient &r) override;

  int getLength();

 private:
  char *dst;
};

class WriteBlockRequest : public StreamControlRequest {
 public:
  WriteBlockRequest(long streamId, const char *data, int len);

  Status write(IGFSClient &w) override;

  int commandId() override;

 private:
  const char *data;
};

class RenameRequest : public PathControlRequest {
 public:
  RenameRequest(const std::string &path, const std::string &destPath);

  int commandId() override;
};

class RenameResponse {
 public:
  RenameResponse();

  Status read(IGFSClient &r);

  bool successful();

 private:
  bool ex{};
};

}

#endif
