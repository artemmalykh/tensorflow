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

Status IgfsClient::openCreate(ControlResponse<OpenCreateResponse> &res, std::string& path) {
  OpenCreateRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  if (res.isOk()) {
    std::cout << "response stream id: " << res.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << res.getErrorCode() << res.getError() << std::endl;
  }

  return Status::OK();
}

Status IgfsClient::openAppend(ControlResponse<OpenAppendResponse> &res, std::string userName, std::string path) {
  OpenAppendRequest req(userName, path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  if (res.isOk()) {
    std::cout << "response stream id: " << res.getRes().getStreamId() << std::endl;
  } else {
    std::cout << "response error code: " << res.getErrorCode() << res.getError() << std::endl;
  }

  return Status::OK();
}

Status IgfsClient::openRead(ControlResponse<Optional<OpenReadResponse>> &res, std::string userName, std::string path) {
  OpenReadRequest req(userName, path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::exists(ControlResponse<ExistsResponse> &res, const std::string& path) {
  ExistsRequest req(path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::mkdir(ControlResponse<MakeDirectoriesResponse> &res, const std::string& path) {
  MakeDirectoriesRequest req("", path);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::del(ControlResponse<DeleteResponse> &res, std::string path, bool recursive) {
  DeleteRequest req(path, recursive);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::writeBlock(long streamId, const char *data, int len) {
  WriteBlockRequest req(streamId, data, len);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::readBlock(ReadBlockControlResponse &res, long streamId, long pos, int len, char* dst) {
  ReadBlockRequest req(streamId, pos, len);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::close(ControlResponse<CloseResponse> &res, long streamId) {
  CloseRequest req(streamId);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}

Status IgfsClient::rename(ControlResponse<RenameResponse> &res, const std::string &source, const std::string &dest) {
  RenameRequest req(source, dest);
  TF_RETURN_IF_ERROR(req.write(*cl));
  cl->reset();

  TF_RETURN_IF_ERROR(res.read(*cl));
  cl->reset();

  return Status::OK();
}
}