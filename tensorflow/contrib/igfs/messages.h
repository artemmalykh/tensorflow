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

namespace tensorflow {

class IgnitePath {
 public:
  void read(Reader &r) {
    path_ = r.readNullableString();
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

  void read(Reader &r);

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

  virtual void write(Writer &w);
};

class Response {
 public:
  Response();

  virtual void read(Reader &r);

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

  void write(Writer &w);

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

  void writePath(Writer &w, string &path);
};

class StreamControlRequest : public Request {
 public:
  StreamControlRequest(long streamId, int len);

  void write(Writer &w) override;

 protected:
  long streamId;

  int len;
};

template<class R>
class ControlResponse : public Response {
 public:
  ControlResponse() = default;

  void read(Reader &r) override {
    Response::read(r);

    if (isOk()) {
      res = R();
      res.read(r);
    }
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
  void read(Reader &r);

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
  void read(Reader &r);

  ExistsResponse();

  bool exists();

 private:
  bool ex;
};

class HandshakeRequest : Request {
 public:
  HandshakeRequest(string fsName, string logDir);

  int commandId() override;

  void write(Writer &w) override;

 private:
  string fsName;
  string logDir;
};

class HandshakeResponse {
 public:
  HandshakeResponse();

  void read(Reader &r);

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
  void read(Reader &r) {
    int len = r.readInt();

    entries = vector<T>();

    for (int i = 0; i < len; i++) {
      T f = {};
      f.read(r);
      entries.push_back(f);
    }
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

 private:
  int commandId() override {
    return 9;
  }
};

class ListPathsResponse : public ListResponse<IgnitePath> {

};

class OpenCreateRequest : PathControlRequest {
 public:
  explicit OpenCreateRequest(const string &path);

  void write(Writer &w) override {
    PathControlRequest::write(w);

    w.writeInt(replication);
    w.writeLong(blockSize);
  }

  int commandId() override {
    return 15;
  }

 protected:
  /** Hadoop replication factor. */
  int replication{};

  /** Hadoop block size. */
  long blockSize{};
};

class OpenCreateResponse {
 public:
  void read(Reader &r) {
    streamId = r.readLong();
  }

  OpenCreateResponse() = default;

  long getStreamId() {
    return streamId;
  }

 private:
  long streamId{};
};

class OpenAppendRequest : PathControlRequest {
 public:
  OpenAppendRequest(const string &userName, const string &path) :
      PathControlRequest(userName, path, "", false, true, map<string, string>()) {}

  void write(Writer &w) override {
    PathControlRequest::write(w);
  }

  int commandId() override {
    return 14;
  }
};

class OpenAppendResponse {
 public:
  void read(Reader &r) {
    streamId = r.readLong();
  }

  OpenAppendResponse() = default;

  long getStreamId() {
    return streamId;
  }

 private:
  long streamId{};
};

class OpenReadRequest : PathControlRequest {
 public:
  OpenReadRequest(const string &userName, const string &path, const string &destPath, bool flag,
                  bool collocate, const map<string, string> &props) : PathControlRequest(userName,
                                                                                         path,
                                                                                         destPath,
                                                                                         flag,
                                                                                         collocate,
                                                                                         props) {}

  void write(Writer &w) override {
    PathControlRequest::write(w);

    if (flag) {
      w.writeInt(seqReadsBeforePrefetch);
    }
  }

  int commandId() override {
    return 13;
  }

 protected:
  /** Sequential reads before prefetch. */
  int seqReadsBeforePrefetch{};
};

class OpenReadResponse {
 public:
  void read(Reader &r) {
    streamId = r.readLong();
    len = r.readLong();
  }

  OpenReadResponse() = default;

  long getStreamId() {
    return streamId;
  }

  long getLength() {
    return len;
  }

 private:
  long streamId{};
  long len{};
};

class InfoRequest : public PathControlRequest {
 public:
  explicit InfoRequest(string &path) : PathControlRequest("", path, "", false, false, map<string, string>()) {}

 private:
  int commandId() override {
    return 3;
  }
};

class InfoResponse {
 public:
  void read(Reader &r) {
    fileInfo = IgfsFile();
    fileInfo.read(r);
  }

  InfoResponse() = default;

  IgfsFile getFileInfo() {
    return fileInfo;
  }

 private:
  IgfsFile fileInfo;
};

class MakeDirectoriesRequest : public PathControlRequest {
 public:
  explicit MakeDirectoriesRequest(const string &path) : PathControlRequest("", path, "", false, false,
                                                                           map<string, string>()) {}

 private:
  int commandId() override {
    return 8;
  }
};

class MakeDirectoriesResponse {
 public:
  void read(Reader &r) {
    succ = r.readBool();
  }

  MakeDirectoriesResponse() = default;

  bool successful() {
    return succ;
  }

 private:
  bool succ{};
};

/** Stream control requests. **/

class CloseRequest : public StreamControlRequest {
 public:
  explicit CloseRequest(long streamId) : StreamControlRequest(streamId, 0) {}

  int commandId() override {
    return 16;
  }
};

class CloseResponse {
 public:
  CloseResponse() : successful(false) {

  }

  void read(Reader &r) {
    successful = r.readBool();
  }

  bool isSuccessful() {
    return successful;
  }

 private:
  bool successful;
};

class ReadBlockRequest : public StreamControlRequest {
 public:
  ReadBlockRequest(long streamId, long pos, int len) : StreamControlRequest(streamId, len), pos(pos) {}

  void write(Writer &w) override {
    StreamControlRequest::write(w);

    w.writeLong(pos);
  }

  int commandId() override {
    return 17;
  }

 private:
  long pos;
};

class ReadBlockResponse {
 public:
  void read(Reader &r, int length, char *dst) {
    successfulyRead = r.readBytes(dst, length);
  }

  void read(Reader &r) {
    // No-op
  }

  streamsize getSuccessfulyRead() {
    return successfulyRead;
  }

 private:
  int length;
  streamsize successfulyRead;
};

class ReadBlockControlResponse : public ControlResponse<ReadBlockResponse> {
 public:
  explicit ReadBlockControlResponse(char *dst) : dst(dst) {}

  void read(Reader &r) override {
    Response::read(r);

    if (isOk()) {
      res = ReadBlockResponse();
      res.read(r, len, dst);
    }
  }

  int getLength() {
    return len;
  }

 private:
  char *dst;
};

class WriteBlockRequest : public StreamControlRequest {
 public:
  WriteBlockRequest(long streamId, const char *data, int len) : StreamControlRequest(streamId, len), data(data) {}

  void write(Writer &w) override {
    StreamControlRequest::write(w);

    w.writeBytes(data, len);
  }

  int commandId() override {
    return 18;
  }

 private:
  const char *data;
};

class WriteBlockResponse {
 public:
  WriteBlockResponse() = default;

  void read(Reader &r) {

  }
};

class RenameRequest : public PathControlRequest {
 public:
  RenameRequest(const std::string &path, const std::string &destPath) : PathControlRequest("", path,
                                                                                           destPath, false,
                                                                                           false,
                                                                                           std::map<std::string, std::string>()) {}

 private:
  int commandId() override {
    return 6;
  }
};

class RenameResponse {
 public:
  void read(Reader &r) {
    ex = r.readBool();
  }

  RenameResponse() = default;

  bool successful() {
    return ex;
  }

 private:
  bool ex{};
};

}

#endif
