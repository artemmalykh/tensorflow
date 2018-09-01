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

namespace tensorflow {

string GetEnvOrElse(const string &env, string default_value) {
  const char *envCStr = env.c_str();
  return getenv(envCStr) != nullptr ? getenv(envCStr) : std::move(default_value);
}

shared_ptr<IgfsProtocolMessenger> CreateClient() {
  string host = GetEnvOrElse("IGFS_HOST", "localhost");
  int port = atoi(GetEnvOrElse("IGFS_PORT", "10500").c_str());
  string fsName = GetEnvOrElse("IGFS_FS_NAME", "myFileSystem");

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
  IGFSRandomAccessFile(string file_name,
                       long resource_id,
                       shared_ptr<IgfsProtocolMessenger>& client)
      : file_name_(std::move(file_name)),
        resource_id_(resource_id),
        client_(client) {}

  ~IGFSRandomAccessFile() override {
    ControlResponse<CloseResponse> cr = {};
    client_->close(&cr, resource_id_);
  }

  Status Read(uint64 offset, size_t n, StringPiece *result,
              char *scratch) const override {

    LOG(INFO) << "Read " << file_name_ << " file";
    Status s;
    char *dst = scratch;

    ReadBlockControlResponse response = ReadBlockControlResponse(dst);
    TF_RETURN_IF_ERROR(
        client_->ReadBlock(&response, resource_id_, offset, n, dst));

    if (!response.IsOk()) {
      s = Status(error::INTERNAL, "Error while trying to read block.");
    } else {
      streamsize sz = response.GetRes().GetSuccessfulyRead();
      *result = StringPiece(scratch, sz);
    }

    return s;
  }

 private:
  shared_ptr<IgfsProtocolMessenger> client_;
  long resource_id_;
  string file_name_;
};

Status IgniteFileSystem::NewRandomAccessFile(
    const string &file_name,
    std::unique_ptr<RandomAccessFile> *result) {
  LOG(INFO) << "New RAF " << file_name << " file";

  shared_ptr<IgfsProtocolMessenger> client = CreateClient();
  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const string path = TranslateName(file_name);

    ControlResponse<Optional<OpenReadResponse>> open_read_response = {};
    TF_RETURN_IF_ERROR(client->openRead(&open_read_response, "", path));

    if (open_read_response.IsOk()) {
      long resource_id = open_read_response.GetRes().Get().GetStreamId();

      result->reset(new IGFSRandomAccessFile(path, resource_id, client));
    } else {
      return Status(error::INTERNAL, "Error while trying to open for reading.");
    }
  }

  return Status::OK();
}

class IGFSWritableFile : public WritableFile {
 public:
  IGFSWritableFile(const string& file_name,
                   long resource_id,
                   shared_ptr<IgfsProtocolMessenger> client)
      : file_name_(file_name), resourceId_(resource_id), client_(client) {
   LOG(INFO) << "Construct new writable file " << file_name;
 }

  ~IGFSWritableFile() override {
    if (resourceId_ >= 0) {
      ControlResponse<CloseResponse> cr = {};
      client_->close(&cr, resourceId_);
    }
  }

  Status Append(const StringPiece &data) override {
    LOG(INFO) << "Append to " << file_name_;
    TF_RETURN_IF_ERROR(client_->writeBlock(
        resourceId_, data.data(), data.size()));

    return Status::OK();
  }

  Status Close() override {
    LOG(INFO) << "Close " << file_name_;
    Status result;

    ControlResponse<CloseResponse> cr = {};
    client_->close(&cr, resourceId_);
    if (!cr.IsOk()) {
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
  string file_name_;
};

Status IgniteFileSystem::NewWritableFile(
    const string &fname,
    std::unique_ptr<WritableFile> *result) {
  LOG(INFO) << "New writable file " << fname;
 
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    string path = TranslateName(fname);

    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> existsResponse = {};
    TF_RETURN_IF_ERROR(client->exists(&existsResponse, path));

    if (existsResponse.IsOk()) {
      if (existsResponse.GetRes().Exists()) {
        ControlResponse<DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->del(&delResponse, path, false));

        if (!delResponse.IsOk()) {
          return Status(
              error::INTERNAL, "Error trying to delete existing file.");
        }
      }
    } else {
      return Status(error::INTERNAL, "Error trying to know if file exists.");
    }

    ControlResponse<OpenCreateResponse> open_create_resp = {};
    TF_RETURN_IF_ERROR(client->openCreate(&open_create_resp, path));

    if (open_create_resp.IsOk()) {
      long resourceId = open_create_resp.GetRes().GetStreamId();

      result->reset(new IGFSWritableFile(path, resourceId, client));
    } else {
      return Status(error::INTERNAL, "Error during open/create request.");
    }
  }

  return Status::OK();
}

Status IgniteFileSystem::NewAppendableFile(
    const string &fname, std::unique_ptr<WritableFile> *result) {
  LOG(INFO) << "New appendable file " << fname;
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> existsResponse = {};
    TF_RETURN_IF_ERROR(client->exists(&existsResponse, fname));

    if (existsResponse.IsOk()) {
      if (existsResponse.GetRes().Exists()) {
        ControlResponse<DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->del(&delResponse, fname, false));

        if (!delResponse.IsOk()) {
          return Status(
              error::INTERNAL, "Error trying to delete existing file.");
        }
      }
    } else {
      return Status(
          error::INTERNAL, "Error trying to know if file exists.");
    }

    ControlResponse<OpenAppendResponse> openAppendResp = {};
    TF_RETURN_IF_ERROR(client->openAppend(&openAppendResp, "", fname));

    if (openAppendResp.IsOk()) {
      long resourceId = openAppendResp.GetRes().GetStreamId();

      result->reset(
          new IGFSWritableFile(TranslateName(fname), resourceId, client));
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
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string path = TranslateName(fname);
    ControlResponse<ExistsResponse> existsResponse = {};
    TF_RETURN_IF_ERROR(client->exists(&existsResponse, path));

    if (existsResponse.IsOk()) {
      if (existsResponse.GetRes().Exists()) {
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
  return string((firstIsShortest ? r.first : r.second) + 1,
                firstIsShortest ? min.end() : max.end());
}

Status IgniteFileSystem::GetChildren(const string &fname,
                                     std::vector<string> *result) {
  LOG(INFO) << "Get children " << fname;
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string dir = TranslateName(fname);

    ControlResponse<ListPathsResponse> listPathsResponse = {};
    TF_RETURN_IF_ERROR(client->listPaths(&listPathsResponse, dir));

    if (!listPathsResponse.IsOk()) {
      return Status(error::INTERNAL, "Error");
    }

    *result = vector<string>();
    vector<IgnitePath> entries = listPathsResponse.GetRes().getEntries();

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
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string path = TranslateName(fname);

    ControlResponse <DeleteResponse> delResp = {};
    TF_RETURN_IF_ERROR(client->del(&delResp, path, false));

    if (!delResp.IsOk()) {
      return Status(error::INTERNAL, "Error");
    }

    if (!delResp.GetRes().exists()) {
      return errors::NotFound(path, " not found.");
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::CreateDir(const string &fname) {
  LOG(INFO) << "Get dir " << fname;
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string dir = TranslateName(fname);

    ControlResponse <MakeDirectoriesResponse> mkDirResponse = {};
    TF_RETURN_IF_ERROR(client->mkdir(&mkDirResponse, dir));

    if (!(mkDirResponse.IsOk() && mkDirResponse.GetRes().IsSuccessful())) {
      return Status(error::INTERNAL, "Error during creating directory.");
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::DeleteDir(const string &dir) {
   LOG(INFO) << "Delete dir " << dir;
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    ControlResponse<ListFilesResponse> listFilesResponse = {};
    client->listFiles(&listFilesResponse, dir);

    if (!listFilesResponse.IsOk()) {
      return Status(error::INTERNAL, "Error");
    } else {
      if (!listFilesResponse.GetRes().getEntries().empty()) {
        return errors::FailedPrecondition(
            "Cannot delete a non-empty directory.");
      } else {
        ControlResponse <DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->del(&delResponse, dir, true));
        
        if (!delResponse.IsOk()) {
          return Status(
              error::INTERNAL,
              "Error while trying to delete directory.");
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
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string path = TranslateName(fname);
    ControlResponse<InfoResponse> infoResponse = {};
    TF_RETURN_IF_ERROR(client->info(&infoResponse, path));

    if (!infoResponse.IsOk()) {
      return Status(error::INTERNAL, "Error while getting info.");
    } else {
      *size = infoResponse.GetRes().getFileInfo().GetFileSize();
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

  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string srcPath = TranslateName(src);
    const string targetPath = TranslateName(target);

    ControlResponse <RenameResponse> renameResp = {};
    TF_RETURN_IF_ERROR(client->rename(&renameResp, srcPath, targetPath));

    if (!renameResp.IsOk()) {
      return Status(error::INTERNAL, "Error while renaming.");
    }

    if (!renameResp.GetRes().IsSuccessful()) {
      return errors::NotFound(srcPath, " not found.");
    }
  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

Status IgniteFileSystem::Stat(const string &fname, FileStatistics *stats) {
  LOG(INFO) << "Stat " << fname;
  shared_ptr<IgfsProtocolMessenger> client = CreateClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->handshake(&hResponse));

  if (hResponse.IsOk()) {
    const string path = TranslateName(fname);
    ControlResponse<InfoResponse> infoResponse = {};
    TF_RETURN_IF_ERROR(client->info(&infoResponse, path));

    if (!infoResponse.IsOk()) {
      return Status(error::INTERNAL, "Error while getting info.");
    } else {
      IgfsFile info = infoResponse.GetRes().getFileInfo();

      *stats = FileStatistics(
          info.GetFileSize(),
          info.GetModificationTime(),
          (info.GetFlags() & 0x1) != 0);
    }

  } else {
    return Status(error::INTERNAL, "Error during handshake.");
  }

  return Status::OK();
}

REGISTER_FILE_SYSTEM("igfs", IgniteFileSystem);
}  // namespace tensorflow
