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

#include <errno.h>
#include <string>
#include <tensorflow/core/lib/core/errors.h>
#include <tensorflow/core/platform/file_system.h>
#include <tensorflow/core/platform/windows/integral_types.h>

#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/strcat.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/error.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/file_system_helper.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/platform/mutex.h"
#include "third_party/hadoop/hdfs.h"
#include "ignite_file_system.h"
#include "client.h"

namespace tensorflow {

shared_ptr<IgfsClient> createClient() {
  string host = "localhost";
  int port = 10500;
  string fsName = "myFileSystem";

  return shared_ptr<IgfsClient>(new IgfsClient(port, host, fsName));
}

IgniteFileSystem::IgniteFileSystem() {}

IgniteFileSystem::~IgniteFileSystem() {}

string IgniteFileSystem::TranslateName(const string &name) const {
  StringPiece scheme, namenode, path;
  io::ParseURI(name, &scheme, &namenode, &path);
  return path.ToString();
}

class IGFSRandomAccessFile : public RandomAccessFile {
 public:
  IGFSRandomAccessFile(const string& fName, long resourceId, shared_ptr<IgfsClient>& client) : fName_(fName), resourceId_(resourceId), client_(client) {}

  ~IGFSRandomAccessFile() override {
    client_->close(resourceId_);
  }

  Status Read(uint64 offset, size_t n, StringPiece *result,
              char *scratch) const override {
    Status s;
    char *dst = scratch;
    // TODO: check how many bytes were read.
    ReadBlockControlResponse response = client_->readBlock(resourceId_, offset, n, dst);

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
  shared_ptr<IgfsClient> client_;
  long resourceId_;
  string fName_;
};

Status IgniteFileSystem::NewRandomAccessFile(
    const string &fname, std::unique_ptr<RandomAccessFile> *result) {
  shared_ptr<IgfsClient> client = createClient();
  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    ControlResponse<Optional<OpenReadResponse>> openCreateResp = client->openRead("", fname);
    if (openCreateResp.isOk()) {
      long resourceId = openCreateResp.getRes().get().getStreamId();

      const string path = TranslateName(fname);
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
  IGFSWritableFile(const string& fName, long resourceId, shared_ptr<IgfsClient> client) : fName_(fName), resourceId_(resourceId), client_(client) {}

  ~IGFSWritableFile() override {
    if (resourceId_ >= 0) {
      client_->close(resourceId_);
    }
  }

  Status Append(const StringPiece &data) override {
    client_->writeBlock(resourceId_, data.data(), data.size());

    return Status::OK();
  }

  Status Close() override {
    Status result;

    if (!client_->close(resourceId_).isOk()) {
      //result = IOError(fName_, errno);
    }

    resourceId_ = -1;

    return result;
  }

  Status Flush() override {
    // TODO: Check.
    return Status::OK();
  }

  Status Sync() override {
    // TODO: Check.
    return Status::OK();
  }

 private:
  shared_ptr<IgfsClient> client_;
  long resourceId_;
  string fName_;
};

Status IgniteFileSystem::NewWritableFile(const string &fname, std::unique_ptr<WritableFile> *result) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> existsResponse = client->exists("", fname);

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        ControlResponse<DeleteResponse> delResponse = client->del(fname, false);

        if (!delResponse.isOk()) {
          // TODO: return error with appropriate code.
          return Status(error::INTERNAL, "Error trying to delete existing file.");
        }
      }
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error trying to know if file exists.");
    }
    
    ControlResponse<OpenCreateResponse> openCreateResp = client->openCreate("", fname);

    if (openCreateResp.isOk()) {
      long resourceId = openCreateResp.getRes().getStreamId();

      const string path = TranslateName(fname);
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
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> existsResponse = client->exists("", fname);

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        ControlResponse<DeleteResponse> delResponse = client->del(fname, false);

        if (!delResponse.isOk()) {
          // TODO: return error with appropriate code.
          return Status(error::INTERNAL, "Error trying to delete existing file.");
        }
      }
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error trying to know if file exists.");
    }

    ControlResponse<OpenAppendResponse> openAppendResp = client->openAppend("", fname);

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
  //TODO:
  return Status::OK();
//  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IgniteFileSystem::FileExists(const string &fname) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    
  } else {
    ControlResponse<ExistsResponse> existsResponse = client->exists("", fname);

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        return Status::OK();
      }
//      return errors::NotFound(fname, " not found.");
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }
}

Status IgniteFileSystem::GetChildren(const string &dir,
                                     std::vector<string> *result) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    client->listFiles(dir);
  } else {
    // TODO: return error with appropriate code.
    return Status(error::INTERNAL, "Error");
  }

  return Status::OK();
}

Status IgniteFileSystem::GetMatchingPaths(const string &pattern,
                                          std::vector<string> *results) {
  //TODO:
  return Status::OK();
//  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IgniteFileSystem::DeleteFile(const string &fname) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    client->del(fname, false);
  } else {
    // TODO: return error with appropriate code.
    return Status(error::INTERNAL, "Error");
  }

  return Status::OK();
}

Status IgniteFileSystem::CreateDir(const string &dir) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    client->mkdir(dir);
  } else {
    // TODO: return error with appropriate code.
    return Status(error::INTERNAL, "Error");
  }

  return Status::OK();
}

Status IgniteFileSystem::DeleteDir(const string &dir) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    ControlResponse<ListFilesResponse> listFilesResponse = client->listFiles(dir);

    // Count the number of entries in the directory, and only delete if it's
    // non-empty. This is consistent with the interface, but note that there's
    // a race condition where a file may be added after this check, in which
    // case the directory will still be deleted.
    if (!listFilesResponse.isOk()) {
      return Status(error::INTERNAL, "Error");
    } else {
      if (!listFilesResponse.getRes().getEntries().empty()) {
//        return errors::FailedPrecondition("Cannot delete a non-empty directory.");
        // TODO: return error with appropriate code.
        return Status(error::INTERNAL, "Error");
      } else {
        client->del(dir, true);
      }
    }

  } else {
    // TODO: return error with appropriate code.
    return Status(error::INTERNAL, "Error");
  }

  return Status::OK();
}

Status IgniteFileSystem::GetFileSize(const string &fname, uint64 *size) {
  //TODO:
  return Status::OK();
//  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IgniteFileSystem::RenameFile(const string &src, const string &target) {
  shared_ptr<IgfsClient> client = createClient();

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    client->rename(src, target);
  } else {
    // TODO: return error with appropriate code.
    return Status(error::INTERNAL, "Error");
  }

  return Status::OK();
}

Status IgniteFileSystem::Stat(const string &fname, FileStatistics *stats) {
  //TODO:
  return Status::OK();
//  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

/////////////

string FileSystem::TranslateName(const string& name) const {
  return "";
}

Status FileSystem::IsDirectory(const string& name) {
  return Status::OK();
}

void FileSystem::FlushCaches() {}

bool FileSystem::FilesExist(const std::vector<string>& files,
                            std::vector<Status>* status) {
  return false;
}

Status FileSystem::DeleteRecursively(const string& dirname,
                                     int64* undeleted_files,
                                     int64* undeleted_dirs) {
  return Status::OK();
}

Status FileSystem::RecursivelyCreateDir(const string& dirname) {
  return Status::OK();
}

Status FileSystem::CopyFile(const string& src, const string& target) {
  return Status::OK();
}

REGISTER_FILE_SYSTEM("hdfs", IgniteFileSystem);
REGISTER_FILE_SYSTEM("viewfs", IgniteFileSystem);

}  // namespace tensorflow
