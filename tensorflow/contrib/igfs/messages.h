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
#include "igfs_file.h"

using namespace std;

namespace tensorflow {

class Request {
 public:
  virtual int commandId() = 0;

  virtual void write(Writer &w) {
    int off = 0;

    w.writeChar(0);
    w.skipToPos(8);
    w.writeInt(commandId());
    w.skipToPos(24);
  }
};

class Response {
 public:
  Response() {

  }

  virtual void read(Reader &r) {
    //read Header
    r.readChar();
    r.skipToPos(8);
    requestId = r.readInt();
    r.skipToPos(24);
    resType = r.readInt();
    bool hasError = r.readBool();

    if (hasError) {
      error = r.readString();
      errorCode = r.readInt();
    } else {
      r.skipToPos(HEADER_SIZE + 6 - 1);
      len = r.readInt();
      r.skipToPos(HEADER_SIZE + RESPONSE_HEADER_SIZE);
    }
  }

  int getResType() {
    return resType;
  }

  int getRequestId() {
    return requestId;
  }

  bool isOk() {
    return errorCode == -1;
  }

  string getError() {
    return error;
  }

  int getErrorCode() {
    return errorCode;
  }

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
                     map<string, string> props) :
      userName(userName), path(path), destPath(destPath), flag(flag), collocate(collocate), props(props) {
  }

  virtual void write(Writer &w) {
    Request::write(w);

    w.writeString(userName);
    writePath(w, path);
    writePath(w, destPath);
    w.writeBoolean(flag);
    w.writeBoolean(collocate);
    w.writeStringMap(props);
  }

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

  void writePath(Writer &w, string path) {
    w.writeBoolean(!path.empty());
    if (!path.empty())
      w.writeString(path);
  }
};

class StreamControlRequest : public Request {
 public:
  StreamControlRequest(long streamId, int len) :
      streamId(streamId),
      len(len) {
  }

  virtual void write(Writer &w) {
    int off = 0;

    w.writeChar(0);
    w.skipToPos(8);
    w.writeInt(commandId());
    w.writeLong(streamId);
    w.writeInt(len);
  }

 protected:
  long streamId;

  int len;
};

template<class R>
class ControlResponse : public Response {
 public:
  ControlResponse() {

  }

  void read(Reader &r) {
    Response::read(r);

    if (isOk()) {
      res = R();
      res.read(r);
    }
  }

 public:
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
  int commandId();
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
  ExistsRequest(const string &userName, const string &path, const string &destPath, bool flag,
                bool collocate, const map<string, string> &props);

  int commandId();
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
 private:
  int requestId;
  string fsName;
  string logDir;

 public:
  HandshakeRequest(string fsName, string logDir) : fsName(fsName), logDir(logDir) {

  }

  int commandId() override {
    return 0;
  }

  void write(Writer &w) {
    Request::write(w);
    w.writeString(fsName);
    w.writeString(logDir);
  }
};

class HandshakeResponse {
 public:
  void read(Reader &r) {
    fsName = r.readNullableString();
    blockSize = r.readLong();

    bool hasSampling = r.readBool();

    if (hasSampling) {
      sampling = r.readBool();
    }
  }

  HandshakeResponse() {

  }

  string getFSName() {
    return fsName;
  }

 private:
  string fsName;
  long blockSize;
  bool sampling;
};

class ListRequest : public PathControlRequest {
 public:
  ListRequest(const string &path) : PathControlRequest("", path, "", false, false, map<string, string>()) {

  }
};

class ListResponse {
 public:
  void read(Reader &r) {
    int len = r.readInt();

    entries = vector<IgfsFile>();

    for (int i = 0; i < len; i++) {
      IgfsFile f = {};
      f.read(r);
      entries.push_back(f);
    }
  }

  vector<IgfsFile> getEntries() {
    return entries;
  }

 protected:
  vector<IgfsFile> entries;
};

class ListFilesRequest : public ListRequest {
 public:
  ListFilesRequest(const string &path) : ListRequest(path) {}

 private:
  int commandId() override {
    return 10;
  }
};

class ListFilesResponse : public ListResponse {

};

class OpenCreateRequest : PathControlRequest {
 public:
  OpenCreateRequest(const string &userName, const string &path, const string &destPath, bool flag,
                    bool collocate, const map<string, string> &props) : PathControlRequest(userName,
                                                                                           path,
                                                                                           destPath,
                                                                                           flag,
                                                                                           collocate,
                                                                                           props) {}

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
  int replication;

  /** Hadoop block size. */
  long blockSize;
};

class OpenCreateResponse {
 public:
  void read(Reader &r) {
    streamId = r.readLong();
  }

  OpenCreateResponse() {

  }

  long getStreamId() {
    return streamId;
  }

 private:
  long streamId;
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

  OpenAppendResponse() {

  }

  long getStreamId() {
    return streamId;
  }

 private:
  long streamId;
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

  void write(Writer &w) {
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
  int seqReadsBeforePrefetch;
};

class OpenReadResponse {
 public:
  void read(Reader &r) {
    streamId = r.readLong();
    len = r.readLong();
  }

  OpenReadResponse() {

  }

  long getStreamId() {
    return streamId;
  }

  long getLength() {
    return len;
  }

 private:
  long streamId;
  long len;
};

class MakeDirectoriesRequest : public PathControlRequest {
 public:
  MakeDirectoriesRequest(string &path) : PathControlRequest("", path, "", false, false, map<string, string>()) {}

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

  MakeDirectoriesResponse() {

  }

  bool successful() {
    return succ;
  }

 private:
  bool succ;
};

/** Stream control requests. **/

class CloseRequest : public StreamControlRequest {
 public:
  CloseRequest(long streamId) : StreamControlRequest(streamId, 0) {}

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

  void write(Writer &w) {
    StreamControlRequest::write(w);

    w.writeLong(pos);
// TODO: check
    w.writeInt(len);
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
    r.readBytes(dst, length);
  }

  void read(Reader &r) {
    // No-op
  }

 private:
  int length;
};

class ReadBlockControlResponse : public ControlResponse<ReadBlockResponse> {
 public:
  ReadBlockControlResponse(char *dst) : dst(dst) {}

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

  void write(Writer &w) {
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
  WriteBlockResponse() {

  }

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

  RenameResponse() {

  }

  bool successful() {
    return ex;
  }

 private:
  bool ex;
};

}

#endif
