#include "messages.h"

using namespace ignite;


void IgfsFile::read(IGFSClient &r) {
  path_ = Optional<IgnitePath>();
  path_.read(r);
  props_ = map<string, string>();

  r.ReadInt(blockSize_);
  r.ReadLong(grpBlockSize_);
  r.ReadLong(len_);
  r.ReadStringMap(props_);
  r.ReadLong(accessTime_);
  r.ReadLong(modificationTime_);
  r.ReadByte(reinterpret_cast<uint8_t &>(flags_));
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

void Request::write(IGFSClient &w) {
  w.WriteByte(0);
  w.skipToPosW(8);
  w.WriteInt(commandId());
  w.skipToPosW(24);
}

Response::Response() {

}

void Response::read(IGFSClient &r) {
  //read Header
  r.Ignore(1);
  r.skipToPos(8);
  r.ReadInt(requestId);
  r.skipToPos(24);
  r.ReadInt(resType);
  bool hasError;
  r.ReadBool(hasError);

  if (hasError) {
    r.ReadString(error);
    r.ReadInt(errorCode);
  } else {
    r.skipToPos(HEADER_SIZE + 6 - 1);
    r.ReadInt(len);
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

void PathControlRequest::write(IGFSClient &w) {
  Request::write(w);

  w.writeString(userName);
  writePath(w, path);
  writePath(w, destPath);
  w.writeBoolean(flag);
  w.writeBoolean(collocate);
  w.writeStringMap(props);
}

void PathControlRequest::writePath(IGFSClient &w, string &path) {
  w.writeBoolean(!path.empty());
  if (!path.empty())
    w.writeString(path);
}

void StreamControlRequest::write(IGFSClient &w) {
  w.WriteByte(0);
  w.skipToPosW(8);
  w.WriteInt(commandId());
  w.WriteLong(streamId);
  w.WriteInt(len);
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

void DeleteResponse::read(IGFSClient &r) {
  r.ReadBool(done);
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

void ExistsResponse::read(IGFSClient &r) {
  r.ReadBool(ex);
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

void HandshakeRequest::write(IGFSClient &w) {
  Request::write(w);

  w.writeString(fsName);
  w.writeString(logDir);
}

HandshakeResponse::HandshakeResponse() = default;

void HandshakeResponse::read(IGFSClient &r) {
  r.ReadNullableString(fsName);
  r.ReadLong(blockSize);

  bool hasSampling;
  r.ReadBool(hasSampling);

  if (hasSampling) {
    r.ReadBool(sampling);
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

void OpenCreateRequest::write(IGFSClient &w) {
  PathControlRequest::write(w);

  w.WriteInt(replication);
  w.WriteLong(blockSize);
}

int OpenCreateRequest::commandId() {
  return 15;
}

void OpenCreateResponse::read(IGFSClient &r) {
  r.ReadLong(streamId);
}

long OpenCreateResponse::getStreamId() {
  return streamId;
}

OpenAppendRequest::OpenAppendRequest(const string &userName, const string &path) :
    PathControlRequest(userName, path, "", false, true, map<string, string>()) {}

void OpenAppendRequest::write(IGFSClient &w) {
  PathControlRequest::write(w);
}

int OpenAppendRequest::commandId() {
  return 14;
}

void OpenAppendResponse::read(IGFSClient &r) {
  r.ReadLong(streamId);
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

void OpenReadRequest::write(IGFSClient &w) {
  PathControlRequest::write(w);

  if (flag) {
    w.WriteInt(seqReadsBeforePrefetch);
  }
}

int OpenReadRequest::commandId() {
  return 13;
}

void OpenReadResponse::read(IGFSClient &r) {
  r.ReadLong(streamId);
  r.ReadLong(len);
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

void InfoResponse::read(IGFSClient &r) {
  fileInfo = IgfsFile();
  fileInfo.read(r);
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

void MakeDirectoriesResponse::read(IGFSClient &r) {
  r.ReadBool(succ);
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

void CloseResponse::read(IGFSClient &r) {
  r.ReadBool(successful);
}

bool CloseResponse::isSuccessful() {
  return successful;
}

ReadBlockRequest::ReadBlockRequest(long streamId, long pos, int len) : StreamControlRequest(streamId, len), pos(pos) {}

void ReadBlockRequest::write(IGFSClient &w) {
  StreamControlRequest::write(w);

  w.WriteLong(pos);
}

int ReadBlockRequest::commandId() {
  return 17;
}

void ReadBlockResponse::read(IGFSClient &r, int length, char *dst) {
  r.ReadData(reinterpret_cast<uint8_t *>(dst), length);
  successfulyRead = length;
}

void ReadBlockResponse::read(IGFSClient &r) {
  // No-op
}

streamsize ReadBlockResponse::getSuccessfulyRead() {
  return successfulyRead;
}

ReadBlockControlResponse::ReadBlockControlResponse(char *dst) : dst(dst) {}

void ReadBlockControlResponse::read(IGFSClient &r) {
  Response::read(r);

  if (isOk()) {
    res = ReadBlockResponse();
    res.read(r, len, dst);
  }
}

int ReadBlockControlResponse::getLength() {
  return len;
}

WriteBlockRequest::WriteBlockRequest(long streamId, const char *data, int len) :
    StreamControlRequest(streamId, len), data(data) {}

void WriteBlockRequest::write(IGFSClient &w) {
  StreamControlRequest::write(w);

  w.WriteData((uint8_t* )data, len);
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

void RenameResponse::read(IGFSClient &r) {
  r.ReadBool(ex);
}

bool RenameResponse::successful() {
  return ex;
}




