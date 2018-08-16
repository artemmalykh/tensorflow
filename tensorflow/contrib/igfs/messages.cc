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

}
