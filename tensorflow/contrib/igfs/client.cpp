#include "client.h"

namespace tensorflow {

IgfsClient::IgfsClient(int port, std::string host, std::string fsName) :
    port(port),
    host(host),
    fsName(fsName) {
  struct sockaddr_in serv_addr;

  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n  \n");
    throw Network("Socket creation error");
  }

  memset(&serv_addr, '0', sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);

  // Convert IPv4 and IPv6 addresses from text to binary form
  if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
    throw Network("Invalid address/ Address not supported");
  }

  if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    throw Network("Connection Failed");
  }

  w = createWriter();
  r = createReader();
}

ControlResponse<Optional<HandshakeResponse>> IgfsClient::handshake() {
  HandshakeRequest req("myFileSystem", "");
  w->flush();

  ControlResponse<Optional<HandshakeResponse>> resp = {};
  resp.read(*r);
  r->reset();

  std::cout << "response fs: " << resp.getRes().get().getFSName() << std::endl;

  return resp;
}

ControlResponse<ListFilesResponse> IgfsClient::listFiles(std::string path) {
  ListFilesRequest req(path);
  w->flush();

  ControlResponse<ListFilesResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<OpenCreateResponse> IgfsClient::openCreate(std::string userName, std::string path) {
  OpenCreateRequest req(userName, path, path, false, false, std::map<std::string, std::string>());
  w->flush();

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

ControlResponse<Optional<OpenReadResponse>> IgfsClient::openRead(std::string userName, std::string path) {
  OpenReadRequest req(userName, path, path, false, false, std::map<std::string, std::string>());
  w->flush();

  ControlResponse<Optional<OpenReadResponse>> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<ExistsResponse> IgfsClient::exists(std::string userName, std::string path) {
  ExistsRequest req(userName, path, path, false, false, std::map<std::string, std::string>());
  w->flush();

  ControlResponse<ExistsResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<MakeDirectoriesResponse> IgfsClient::mkdir(std::string path) {
  MakeDirectoriesRequest req(path);
  w->flush();

  ControlResponse<MakeDirectoriesResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<DeleteResponse> IgfsClient::del(std::string path, bool recursive) {
  DeleteRequest req(path, recursive);
  w->flush();

  ControlResponse<DeleteResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

void IgfsClient::writeBlock(long streamId, const char *data, int len) {
  WriteBlockRequest req(streamId, data, len);
  w->flush();
}

ReadBlockControlResponse IgfsClient::readBlock(long streamId, long pos, int len, char* dst) {
  ReadBlockRequest req(streamId, pos, len);
  req.write(*w);
  w->flush();

  ReadBlockControlResponse resp(dst);
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<CloseResponse> IgfsClient::close(long streamId) {
  CloseRequest req(streamId);
  req.write(*w);
  w->flush();

  ControlResponse<CloseResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

ControlResponse<RenameResponse> IgfsClient::rename(const std::string &source, const std::string &dest) {
  RenameRequest req(source, dest);
  w->flush();

  ControlResponse<RenameResponse> resp = {};
  resp.read(*r);
  r->reset();

  return resp;
}

Writer *IgfsClient::createWriter() {
  auto *sBuf1 = new Socketbuf(sock);
  auto *out = new std::ostream(sBuf1);
  auto w = new Writer(out);

  return w;
}

Reader *IgfsClient::createReader() {
  auto *sBuf2 = new Socketbuf(sock);
  auto *in = new std::istream(sBuf2);
  auto r = new Reader(in);

  return r;
}
}