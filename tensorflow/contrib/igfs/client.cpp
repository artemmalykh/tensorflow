#include "client.h"
#include "ignite_plain_client.h"

namespace tensorflow {

IgfsClient::IgfsClient(int port, std::string host, std::string fsName) : plainClient(host, port), fsName(fsName) {
  plainClient.Connect();

  w = createWriter();
  r = createReader();
}

ControlResponse<Optional<HandshakeResponse>> IgfsClient::handshake() {
  HandshakeRequest req(fsName, "");
  req.write(*w);
  w->reset();

  ControlResponse<Optional<HandshakeResponse>> resp = {};
  resp.read(*r);
  r->reset();

  std::cout << "response fs: " << resp.getRes().get().getFSName() << std::endl;

  return resp;
}

ControlResponse<ListFilesResponse> IgfsClient::listFiles(std::string path) {
  ListFilesRequest req(path);
  req.write(*w);
  w->reset();

  ControlResponse<ListFilesResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<ListPathsResponse> IgfsClient::listPaths(std::string path) {
  ListPathsRequest req(path);
  req.write(*w);
  w->reset();

  ControlResponse<ListPathsResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<InfoResponse> IgfsClient::info(std::string path) {
  InfoRequest req("", path);
  req.write(*w);
  w->reset();

  ControlResponse<InfoResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<OpenCreateResponse> IgfsClient::openCreate(std::string& path) {
  OpenCreateRequest req(path);
  req.write(*w);
  w->reset();

  ControlResponse<OpenCreateResponse> resp = {};
  resp.read(*r);
  r->reset();

  if (resp.isOk()) {
    std::cout << "response stream id: " << resp.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << resp.getErrorCode() << resp.getError() << std::endl;
  }

  return resp;
}

ControlResponse<OpenAppendResponse> IgfsClient::openAppend(std::string userName, std::string path) {
  OpenAppendRequest req(userName, path);
  req.write(*w);
  w->reset();

  ControlResponse<OpenAppendResponse> resp = {};
  resp.read(*r);
  r->reset();

  if (resp.isOk()) {
    std::cout << "response stream id: " << resp.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << resp.getErrorCode() << resp.getError() << std::endl;
  }

  return resp;
}

ControlResponse<Optional<OpenReadResponse>> IgfsClient::openRead(std::string userName, std::string path) {
  OpenReadRequest req(userName, path);
  req.write(*w);
  w->reset();

  ControlResponse<Optional<OpenReadResponse>> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<ExistsResponse> IgfsClient::exists(const std::string& path) {
  ExistsRequest req(path);
  req.write(*w);
  w->reset();

  ControlResponse<ExistsResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<MakeDirectoriesResponse> IgfsClient::mkdir(const std::string& path) {
  MakeDirectoriesRequest req("", path);
  req.write(*w);
  w->reset();

  ControlResponse<MakeDirectoriesResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<DeleteResponse> IgfsClient::del(std::string path, bool recursive) {
  DeleteRequest req(path, recursive);
  req.write(*w);
  w->reset();

  ControlResponse<DeleteResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

void IgfsClient::writeBlock(long streamId, const char *data, int len) {
  WriteBlockRequest req(streamId, data, len);
  req.write(*w);
  w->reset();
}

ReadBlockControlResponse IgfsClient::readBlock(long streamId, long pos, int len, char* dst) {
  ReadBlockRequest req(streamId, pos, len);
  req.write(*w);
  w->reset();

  ReadBlockControlResponse resp(dst);
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<CloseResponse> IgfsClient::close(long streamId) {
  CloseRequest req(streamId);
  req.write(*w);
  w->reset();

  ControlResponse<CloseResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<RenameResponse> IgfsClient::rename(const std::string &source, const std::string &dest) {
  RenameRequest req(source, dest);
  req.write(*w);
  w->reset();

  ControlResponse<RenameResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

Writer *IgfsClient::createWriter() {
  auto w = new Writer(plainClient);

  return w;
}

Reader *IgfsClient::createReader() {
  auto r = new Reader(plainClient);

  return r;
}
}