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

#include <memory>
#include <string>
#include <utility>
#include "tensorflow/core/platform/file_system.h"

#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/strcat.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/error.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/file_system_helper.h"
#include "tensorflow/core/platform/logging.h"
#include "ignite_file_system.h"
#include "igfs_protocol.h"

using namespace std;

namespace tensorflow {

string getEnvOrElse(const string &env, string defVal) {
  const char *envCStr = env.c_str();
  return getenv(envCStr) != nullptr ? getenv(envCStr) : std::move(defVal);
}

shared_ptr<IgfsProtocolMessenger> createClient() {
  string host = getEnvOrElse("IGFS_HOST", "localhost");
  int port = atoi(getEnvOrElse("IGFS_PORT", "10500").c_str());
  string fsName = getEnvOrElse("IGFS_FS_NAME", "myFileSystem");

  return std::make_shared<IgfsProtocolMessenger>(port, host, fsName);
}

IgniteFileSystem::IgniteFileSystem() {
    LOG(INFO) << "Construct new Ignite File System";
};

IgniteFileSystem::~IgniteFileSystem() = default;

string IgniteFileSystem::TranslateName(const string &name) const {
  StringPiece scheme, namenode, path;
  io::ParseURI(name, &scheme, &namenode, &path);
  return path.ToString();
}

class IGFSRandomAccessFile : public RandomAccessFile {
 public:
  IGFSRandomAccessFile(const string& fName, long resourceId, shared_ptr<IgfsProtocolMessenger>& client) : fName_(fName), resourceId_(resourceId), client_(client) {}

  ~IGFSRandomAccessFile() override {
    ControlResponse<CloseResponse> cr = {};
    client_->close(&cr, resourceId_);
  }

  Status Read(uint64 offset, size_t n, StringPiece *result,
              char *scratch) const override {

    LOG(INFO) << "Read " << fName_ << " file";
    Status s;
    char *dst = scratch;
    // TODO: check how many bytes were read.
    ReadBlockControlResponse response = ReadBlockControlResponse(dst);
    TF_RETURN_IF_ERROR(client_->readBlock(&response, resourceId_, offset, n, dst));

    if (!response.isOk()) {
      // TODO: Make error return right status;
      s = Status(error::INTERNAL, "Error");
    } else {
      streamsize sz = response.getRes().getSuccessfulyRead();
      *result = StringPiece(scratch, sz);
    }

    return s;
  }

 private:
  shared_ptr<IgfsProtocolMessenger> client_;
  long resourceId_;
  string fName_;
};

Status IgniteFileSystem::NewRandomAccessFile(
    const string &fname, std::unique_ptr<RandomAccessFile> *result) {
  LOG(INFO) << "New RAF " << fName_ << " file";

  shared_ptr<IgfsProtocolMessenger> client = createClient();
  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string path = TranslateName(fname);

    ControlResponse<Optional<OpenReadResponse>> openCreateResp = {};
    TF_RETURN_IF_ERROR(client->openRead(&openCreateResp, "", path));

    if (openCreateResp.isOk()) {
      long resourceId = openCreateResp.getRes().get().getStreamId();

      result->reset(new IGFSRandomAccessFile(path, resourceId, client));
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }

  return Status::OK();
}

class IGFSWritableFile : public WritableFile {
 public:
  IGFSWritableFile(const string& fName, long resourceId, shared_ptr<IgfsProtocolMessenger> client) : fName_(fName), resourceId_(resourceId), client_(client) {
   LOG(INFO) << "Construct new writable file " << fName;
 }

  ~IGFSWritableFile() override {
    if (resourceId_ >= 0) {
      ControlResponse<CloseResponse> cr = {};
      client_->close(&cr, resourceId_);
    }
  }

  Status Append(const StringPiece &data) override {
    LOG(INFO) << "Append to " << fName_;
    TF_RETURN_IF_ERROR(client_->writeBlock(resourceId_, data.data(), data.size()));

    return Status::OK();
  }

  Status Close() override {
    LOG(INFO) << "Close " << fName_;
    Status result;

    ControlResponse<CloseResponse> cr = {};
    client_->close(&cr, resourceId_);
    if (!cr.isOk()) {
      //result = IOError(fName_, errno);
    }

    resourceId_ = -1;

    return result;
  }

  Status Flush() override {
    return Status::OK();
  }

  Status Sync() override {
    return Status::OK();
  }

 private:
  shared_ptr<IgfsProtocolMessenger> client_;
  long resourceId_;
  string fName_;
};

Status IgniteFileSystem::NewWritableFile(const string &fname, std::unique_ptr<WritableFile> *result) {
  LOG(INFO) << "New writable file " << fname;
 
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    string path = TranslateName(fname);

    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> existsResponse = {};
    TF_RETURN_IF_ERROR(client->exists(&existsResponse, path));

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        ControlResponse<DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->del(&delResponse, path, false));

        if (!delResponse.isOk()) {
          return Status(error::INTERNAL, "Error trying to delete existing file.");
        }
      }
    } else {
      return Status(error::INTERNAL, "Error trying to know if file exists.");
    }

    ControlResponse<OpenCreateResponse> openCreateResp = {};
    TF_RETURN_IF_ERROR(client->openCreate(&openCreateResp, path));

    if (openCreateResp.isOk()) {
      long resourceId = openCreateResp.getRes().getStreamId();

      result->reset(new IGFSWritableFile(path, resourceId, client));
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }

  return Status::OK();
}

Status IgniteFileSystem::NewAppendableFile(
    const string &fname, std::unique_ptr<WritableFile> *result) {
  LOG(INFO) << "New appendable file " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> existsResponse = {};
    TF_RETURN_IF_ERROR(client->exists(&existsResponse, fname));

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        ControlResponse<DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->del(&delResponse, fname, false));

        if (!delResponse.isOk()) {
          return Status(error::INTERNAL, "Error trying to delete existing file.");
        }
      }
    } else {
      return Status(error::INTERNAL, "Error trying to know if file exists.");
    }

    ControlResponse<OpenAppendResponse> openAppendResp = {};
    TF_RETURN_IF_ERROR(client->openAppend(&openAppendResp, "", fname));

    if (openAppendResp.isOk()) {
      long resourceId = openAppendResp.getRes().getStreamId();

      result->reset(new IGFSWritableFile(TranslateName(fname), resourceId, client));
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }

  return Status::OK();
}

Status IgniteFileSystem::NewReadOnlyMemoryRegionFromFile(
    const string &fname, std::unique_ptr<ReadOnlyMemoryRegion> *result) {
  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IgniteFileSystem::FileExists(const string &fname) {
  LOG(INFO) << "File exists " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string path = TranslateName(fname);
    ControlResponse<ExistsResponse> existsResponse = {};
    TF_RETURN_IF_ERROR(client->exists(&existsResponse, path));

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        return Status::OK();
      }

      return errors::NotFound(path, " not found.");
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  } else {
    return Status(error::INTERNAL, "Error");
  }
}

string makeRelative(const string &a, const string &b) {
  string max = a;
  string min = b;
  bool firstIsShortest = false;

  if (b.size() > a.size()) {
    max = b;
    min = a;
    firstIsShortest = true;
  }

  auto r = mismatch(min.begin(), min.end(), max.begin());

  // Trim common prefix and '/' (hence '+1')
  return string((firstIsShortest ? r.first : r.second) + 1, firstIsShortest ? min.end() : max.end());
}

Status IgniteFileSystem::GetChildren(const string &fname,
                                     std::vector<string> *result) {
  LOG(INFO) << "Get children " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string dir = TranslateName(fname);

    ControlResponse<ListPathsResponse> listPathsResponse = {};
    TF_RETURN_IF_ERROR(client->listPaths(&listPathsResponse, dir));

    if (!listPathsResponse.isOk()) {
      return Status(error::INTERNAL, "Error");
    }

    *result = vector<string>();
    vector<IgnitePath> entries = listPathsResponse.getRes().getEntries();

    for(auto& value: entries) {
      result->push_back(makeRelative(value.getPath(), dir));
    }
  } else {
    // TODO: return error with appropriate code.
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::GetMatchingPaths(const string& pattern,
                                          std::vector<string>* results) {
  return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
}

Status IgniteFileSystem::DeleteFile(const string &fname) {
  LOG(INFO) << "Delete file " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string path = TranslateName(fname);

    ControlResponse <DeleteResponse> delResp = {};
    TF_RETURN_IF_ERROR(client->del(&delResp, path, false));

    if (!delResp.isOk()) {
      return Status(error::INTERNAL, "Error");
    }

    if (!delResp.getRes().exists()) {
      return errors::NotFound(path, " not found.");
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::CreateDir(const string &fname) {
  LOG(INFO) << "Get dir " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string dir = TranslateName(fname);

    ControlResponse <MakeDirectoriesResponse> mkDirResponse = {};
    TF_RETURN_IF_ERROR(client->mkdir(&mkDirResponse, dir));

    if (!(mkDirResponse.isOk() && mkDirResponse.getRes().successful())) {
      return Status(error::INTERNAL, "Error during creating directory.");
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::DeleteDir(const string &dir) {
   LOG(INFO) << "Delete dir " << dir;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    ControlResponse<ListFilesResponse> listFilesResponse = {};
    client->listFiles(&listFilesResponse, dir);

    if (!listFilesResponse.isOk()) {
      return Status(error::INTERNAL, "Error");
    } else {
      if (!listFilesResponse.getRes().getEntries().empty()) {
        return errors::FailedPrecondition("Cannot delete a non-empty directory.");
      } else {
        ControlResponse <DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->del(&delResponse, dir, true));
        
        if (!delResponse.isOk()) {
          return Status(error::INTERNAL, "Error while trying to delete directory.");
        }
      }
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::GetFileSize(const string &fname, uint64 *size) {
  LOG(INFO) << "Get File size " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string path = TranslateName(fname);
    ControlResponse<InfoResponse> infoResponse = {};
    TF_RETURN_IF_ERROR(client->info(&infoResponse, path));

    if (!infoResponse.isOk()) {
      return Status(error::INTERNAL, "Error while getting info.");
    } else {
      *size = infoResponse.getRes().getFileInfo().getFileSize();
    }

  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::RenameFile(const string &src, const string &target) {
  LOG(INFO) << "Rename file " << src;
  if (FileExists(target).ok()) {
    DeleteFile(target);
  }

  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string srcPath = TranslateName(src);
    const string targetPath = TranslateName(target);

    ControlResponse <RenameResponse> renameResp = {};
    TF_RETURN_IF_ERROR(client->rename(&renameResp, srcPath, targetPath));

    if (!renameResp.isOk()) {
      return Status(error::INTERNAL, "Error while renaming.");
    }

    if (!renameResp.getRes().successful()) {
      return errors::NotFound(srcPath, " not found.");
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::Stat(const string &fname, FileStatistics *stats) {
  LOG(INFO) << "Stat " << fname;
  shared_ptr<IgfsProtocolMessenger> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.isOk()) {
    const string path = TranslateName(fname);
    ControlResponse<InfoResponse> infoResponse = {};
    TF_RETURN_IF_ERROR(client->info(&infoResponse, path));

    if (!infoResponse.isOk()) {
      return Status(error::INTERNAL, "Error while getting info.");
    } else {
      IgfsFile info = infoResponse.getRes().getFileInfo();

      *stats = FileStatistics(info.getFileSize(), info.getModificationTime(), (info.getFlags() & 0x1) != 0);
    }

  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

REGISTER_FILE_SYSTEM("igfs", IgniteFileSystem);
}  // namespace tensorflow
