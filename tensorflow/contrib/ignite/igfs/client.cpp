#include "client.h"
#include "../kernels/ignite_plain_client.h"

namespace ignite {

IgfsClient::IgfsClient(int port, std::string host, std::string fsName) : fsName(fsName) {
  cl = new IGFSClient(host, port);
  cl->Connect();
}

IgfsClient::~IgfsClient() {
  cl->Disconnect();
}

Status IgfsClient::handshake(ControlResponse<Optional<HandshakeResponse>>& res) {
  HandshakeRequest req(fsName, "");
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  std::cout << "response fs: " << res.getRes().get().getFSName() << std::endl;

  return Status::OK();
}

Status IgfsClient::listFiles(ControlResponse<ListFilesResponse>& res, std::string path) {
  ListFilesRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::listPaths(ControlResponse<ListPathsResponse> &res, std::string path) {
  ListPathsRequest req(path);
  req.write(*cl);
  cl->reset();

  res.read(*cl);
  cl->reset();

  return Status::OK();
}

Status IgfsClient::info(ControlResponse<InfoResponse> &res, std::string path) {
  InfoRequest req("", path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

ControlResponse<OpenCreateResponse> IgfsClient::openCreate(std::string& path) {
  OpenCreateRequest req(path);
  req.write(*cl);
  cl->reset();

  ControlResponse<OpenCreateResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  if (resp.isOk()) {
    std::cout << "response stream id: " << resp.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << resp.getErrorCode() << resp.getError() << std::endl;
  }

  return resp;
}

ControlResponse<OpenAppendResponse> IgfsClient::openAppend(std::string userName, std::string path) {
  OpenAppendRequest req(userName, path);
  req.write(*cl);
  cl->reset();

  ControlResponse<OpenAppendResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  if (resp.isOk()) {
    std::cout << "response stream id: " << resp.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << resp.getErrorCode() << resp.getError() << std::endl;
  }

  return resp;
}

ControlResponse<Optional<OpenReadResponse>> IgfsClient::openRead(std::string userName, std::string path) {
  OpenReadRequest req(userName, path);
  req.write(*cl);
  cl->reset();

  ControlResponse<Optional<OpenReadResponse>> resp = {};
  resp.read(*cl);
  cl->reset();

  return resp;
}

ControlResponse<ExistsResponse> IgfsClient::exists(const std::string& path) {
  ExistsRequest req(path);
  req.write(*cl);
  cl->reset();

  ControlResponse<ExistsResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  return resp;
}

ControlResponse<MakeDirectoriesResponse> IgfsClient::mkdir(const std::string& path) {
  MakeDirectoriesRequest req("", path);
  req.write(*cl);
  cl->reset();

  ControlResponse<MakeDirectoriesResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  return resp;
}

ControlResponse<DeleteResponse> IgfsClient::del(std::string path, bool recursive) {
  DeleteRequest req(path, recursive);
  req.write(*cl);
  cl->reset();

  ControlResponse<DeleteResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  return resp;
}

void IgfsClient::writeBlock(long streamId, const char *data, int len) {
  WriteBlockRequest req(streamId, data, len);
  req.write(*cl);
  cl->reset();
}

ReadBlockControlResponse IgfsClient::readBlock(long streamId, long pos, int len, char* dst) {
  ReadBlockRequest req(streamId, pos, len);
  req.write(*cl);
  cl->reset();

  ReadBlockControlResponse resp(dst);
  resp.read(*cl);
  cl->reset();

  return resp;
}

ControlResponse<CloseResponse> IgfsClient::close(long streamId) {
  CloseRequest req(streamId);
  req.write(*cl);
  cl->reset();

  ControlResponse<CloseResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  return resp;
}

ControlResponse<RenameResponse> IgfsClient::rename(const std::string &source, const std::string &dest) {
  RenameRequest req(source, dest);
  req.write(*cl);
  cl->reset();

  ControlResponse<RenameResponse> resp = {};
  resp.read(*cl);
  cl->reset();

  return resp;
}
}