#include "messages.h"

namespace tensorflow {

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

ExistsRequest::ExistsRequest(const std::string &userName, const std::string &path, const std::string &destPath,
                             bool flag,
                             bool collocate, const std::map<std::string, std::string> &props) : PathControlRequest(
    userName, path,
    destPath, flag,
    collocate,
    props) {}

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

}
