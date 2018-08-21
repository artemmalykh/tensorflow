#include "messages.h"

namespace tensorflow {

void IgfsFile::read(Reader &r) {
  path_ = Optional<IgnitePath>();
  path_.read(r);

  blockSize_ = r.readInt();
  grpBlockSize_ = r.readLong();
  len_ = r.readLong();
  props_ = r.readStringMap();
  accessTime_ = r.readLong();
  modificationTime_ = r.readLong();
  flags_ = r.readChar();
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

void Request::write(Writer &w) {
  int off = 0;

  w.writeChar(0);
  w.skipToPos(8);
  w.writeInt(commandId());
  w.skipToPos(24);
}

Response::Response() {

}

void Response::read(Reader &r) {
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

void PathControlRequest::write(Writer &w) {
  Request::write(w);

  w.writeString(userName);
  writePath(w, path);
  writePath(w, destPath);
  w.writeBoolean(flag);
  w.writeBoolean(collocate);
  w.writeStringMap(props);
}

void PathControlRequest::writePath(Writer &w, string &path) {
  w.writeBoolean(!path.empty());
  if (!path.empty())
    w.writeString(path);
}

void StreamControlRequest::write(Writer &w) {
  w.writeChar(0);
  w.skipToPos(8);
  w.writeInt(commandId());
  w.writeLong(streamId);
  w.writeInt(len);
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

void DeleteResponse::read(Reader &r) {
  done = r.readBool();
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

void ExistsResponse::read(Reader &r) {
  ex = r.readBool();
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

void HandshakeRequest::write(Writer &w) {
  Request::write(w);

  w.writeString(fsName);
  w.writeString(logDir);
}

HandshakeResponse::HandshakeResponse() = default;

void HandshakeResponse::read(Reader &r) {
  fsName = r.readNullableString();
  blockSize = r.readLong();

  bool hasSampling = r.readBool();

  if (hasSampling) {
    sampling = r.readBool();
  }
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

void OpenCreateRequest::write(Writer &w) {
  PathControlRequest::write(w);

  w.writeInt(replication);
  w.writeLong(blockSize);
}

int OpenCreateRequest::commandId() {
  return 15;
}

void OpenCreateResponse::read(Reader &r) {
  streamId = r.readLong();
}

long OpenCreateResponse::getStreamId() {
  return streamId;
}

OpenAppendRequest::OpenAppendRequest(const string &userName, const string &path) :
    PathControlRequest(userName, path, "", false, true, map<string, string>()) {}

void OpenAppendRequest::write(Writer &w) {
  PathControlRequest::write(w);
}

int OpenAppendRequest::commandId() {
  return 14;
}

void OpenAppendResponse::read(Reader &r) {
  streamId = r.readLong();
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

void OpenReadRequest::write(Writer &w) {
  PathControlRequest::write(w);

  if (flag) {
    w.writeInt(seqReadsBeforePrefetch);
  }
}

int OpenReadRequest::commandId() {
  return 13;
}

void OpenReadResponse::read(Reader &r) {
  streamId = r.readLong();
  len = r.readLong();
}

OpenReadResponse::OpenReadResponse() = default;

long OpenReadResponse::getStreamId() {
  return streamId;
}

InfoRequest::InfoRequest(const string &userName, const string &path) : PathControlRequest(userName, path, "", false, false,
                                                                              map<string, string>()) {}

int InfoRequest::commandId() {
  return 3;
}

void InfoResponse::read(Reader &r) {
  fileInfo = IgfsFile();
  fileInfo.read(r);
}

IgfsFile InfoResponse::getFileInfo() {
  return fileInfo;
}

}




