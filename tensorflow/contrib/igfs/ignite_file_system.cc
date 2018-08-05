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

IgniteFileSystem::IgniteFileSystem() {}

IgniteFileSystem::~IgniteFileSystem() {}

string IgniteFileSystem::TranslateName(const string &name) const {
  StringPiece scheme, namenode, path;
  io::ParseURI(name, &scheme, &namenode, &path);
  return path.ToString();
}

class IGFSRandomAccessFile : public RandomAccessFile {
 public:
  IGFSRandomAccessFile(const string& fName, long resourceId, unique_ptr<IgfsClient>& client) : fName_(fName), resourceId_(resourceId), client_(client) {}

  ~HDFSRandomAccessFile() override {
    client_->close(resourceId_);
  }

  Status Read(uint64 offset, size_t n, StringPiece *result,
              char *scratch) const override {
    Status s;
    char *dst = scratch;
    bool eof_retried = false;

    // TODO: check how many bytes were read.
    ReadBlockControlResponse response = client_->readBlock(resourceId_, offset, n, dst);

    if (!response.isOk()) {
      // TODO: Make error return right status;
      s = Status(error::INTERNAL, "Error");
    } else {
      *result = StringPiece(scratch, dst - scratch);
    }

    return s;
  }

 private:
  unique_ptr<IgfsClient> client_;
  long resourceId_;
  string fName_;
};

Status IgniteFileSystem::NewRandomAccessFile(
    const string &fname, std::unique_ptr<RandomAccessFile> *result) {
  // TODO: Get actual host and port;
  string host = "localhost";
  int port = 10500;
  string fsName = "myFileSystem";

  unique_ptr<IgfsClient> client = new IgfsClient(port, host, fsName);
  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    ControlResponse<Optional<OpenReadResponse>> openCreateResp = client->openRead("", fname);
    if (openCreateResp.isOk()) {
      long resourceId = openCreateResp.getRes().get().getStreamId();

      result->reset(new IGFSRandomAccessFile(fname, resourceId, client));
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }

  return Status::OK();
}

class IGFSWritableFile : public WritableFile {
 public:
  IGFSWritableFile(const string& fName, long resourceId, unique_ptr<IgfsClient>& client) : fName_(fName), resourceId_(resourceId), client_(client) {}

  ~HDFSWritableFile() override {
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
      result = IOError(fName_, errno);
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
  unique_ptr<IgfsClient> client_;
  long resourceId_;
  string fName_;
};

Status IgniteFileSystem::NewWritableFile(const string &fname, std::unique_ptr<WritableFile> *result) {
  unique_ptr<IgfsClient> client = createClient(fname);

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
    
    ControlResponse<Optional<OpenReadResponse>> openCreateResp = client->openRead("", fname);

    if (openCreateResp.isOk()) {
      long resourceId = openCreateResp.getRes().get().getStreamId();

      result->reset(new IGFSWritableFile(fname, resourceId, client));
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }

  return Status::OK();
}

IgfsClient* createClient(const string& fname) {
  string host = "localhost";
  int port = 10500;
  string fsName = "myFileSystem";

  return new IgfsClient(port, host, fsName);
}

Status IgniteFileSystem::NewAppendableFile(
    const string &fname, std::unique_ptr<WritableFile> *result) {
  unique_ptr<IgfsClient> client = createClient(fname);

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

    ControlResponse<Optional<OpenAppendResponse>> openAppendResp = client->openAppend("", fname);

    if (openAppendResp.isOk()) {
      long resourceId = openAppendResp.getRes().get().getStreamId();

      result->reset(new IGFSWritableFile(fname, resourceId, client));
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }

  return Status::OK();
}

Status HadoopFileSystem::NewReadOnlyMemoryRegionFromFile(
    const string &fname, std::unique_ptr<ReadOnlyMemoryRegion> *result) {
  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IgniteFileSystem::FileExists(const string &fname) {
  unique_ptr<IgfsClient> client = createClient(fname);

  ControlResponse<Optional<HandshakeResponse>> hResponse = client->handshake();

  if (hResponse.isOk()) {
    
  } else {
    ControlResponse<ExistsResponse> existsResponse = client->exists("", fname);

    if (existsResponse.isOk()) {
      if (existsResponse.getRes().exists()) {
        return Status::OK();
      }
      return errors::NotFound(fname, " not found.");
    } else {
      // TODO: return error with appropriate code.
      return Status(error::INTERNAL, "Error");
    }
  }
}

Status IgniteFileSystem::GetChildren(const string &dir,
                                     std::vector<string> *result) {
  result->clear();
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(dir, &fs));

  // hdfsListDirectory returns nullptr if the directory is empty. Do a separate
  // check to verify the directory exists first.
  FileStatistics stat;
  TF_RETURN_IF_ERROR(Stat(dir, &stat));

  int entries = 0;
  hdfsFileInfo *info =
      hdfs_->hdfsListDirectory(fs, TranslateName(dir).c_str(), &entries);
  if (info == nullptr) {
    if (stat.is_directory) {
      // Assume it's an empty directory.
      return Status::OK();
    }
    return IOError(dir, errno);
  }
  for (int i = 0; i < entries; i++) {
    result->push_back(io::Basename(info[i].mName).ToString());
  }
  hdfs_->hdfsFreeFileInfo(info, entries);
  return Status::OK();
}

Status HadoopFileSystem::GetMatchingPaths(const string &pattern,
                                          std::vector<string> *results) {
  return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
}

Status HadoopFileSystem::DeleteFile(const string &fname) {
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(fname, &fs));

  if (hdfs_->hdfsDelete(fs, TranslateName(fname).c_str(),
      /*recursive=*/0) != 0) {
    return IOError(fname, errno);
  }
  return Status::OK();
}

Status HadoopFileSystem::CreateDir(const string &dir) {
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(dir, &fs));

  if (hdfs_->hdfsCreateDirectory(fs, TranslateName(dir).c_str()) != 0) {
    return IOError(dir, errno);
  }
  return Status::OK();
}

Status HadoopFileSystem::DeleteDir(const string &dir) {
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(dir, &fs));

  // Count the number of entries in the directory, and only delete if it's
  // non-empty. This is consistent with the interface, but note that there's
  // a race condition where a file may be added after this check, in which
  // case the directory will still be deleted.
  int entries = 0;
  hdfsFileInfo *info =
      hdfs_->hdfsListDirectory(fs, TranslateName(dir).c_str(), &entries);
  if (info != nullptr) {
    hdfs_->hdfsFreeFileInfo(info, entries);
  }
  // Due to HDFS bug HDFS-8407, we can't distinguish between an error and empty
  // folder, expscially for Kerberos enable setup, EAGAIN is quite common when
  // the call is actually successful. Check again by Stat.
  if (info == nullptr && errno != 0) {
    FileStatistics stat;
    TF_RETURN_IF_ERROR(Stat(dir, &stat));
  }

  if (entries > 0) {
    return errors::FailedPrecondition("Cannot delete a non-empty directory.");
  }
  if (hdfs_->hdfsDelete(fs, TranslateName(dir).c_str(),
      /*recursive=*/1) != 0) {
    return IOError(dir, errno);
  }
  return Status::OK();
}

Status HadoopFileSystem::GetFileSize(const string &fname, uint64 *size) {
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(fname, &fs));

  hdfsFileInfo *info = hdfs_->hdfsGetPathInfo(fs, TranslateName(fname).c_str());
  if (info == nullptr) {
    return IOError(fname, errno);
  }
  *size = static_cast<uint64>(info->mSize);
  hdfs_->hdfsFreeFileInfo(info, 1);
  return Status::OK();
}

Status HadoopFileSystem::RenameFile(const string &src, const string &target) {
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(src, &fs));

  if (hdfs_->hdfsExists(fs, TranslateName(target).c_str()) == 0 &&
      hdfs_->hdfsDelete(fs, TranslateName(target).c_str(),
          /*recursive=*/0) != 0) {
    return IOError(target, errno);
  }

  if (hdfs_->hdfsRename(fs, TranslateName(src).c_str(),
                        TranslateName(target).c_str()) != 0) {
    return IOError(src, errno);
  }
  return Status::OK();
}

Status HadoopFileSystem::Stat(const string &fname, FileStatistics *stats) {
  hdfsFS fs = nullptr;
  TF_RETURN_IF_ERROR(Connect(fname, &fs));

  hdfsFileInfo *info = hdfs_->hdfsGetPathInfo(fs, TranslateName(fname).c_str());
  if (info == nullptr) {
    return IOError(fname, errno);
  }
  stats->length = static_cast<int64>(info->mSize);
  stats->mtime_nsec = static_cast<int64>(info->mLastMod) * 1e9;
  stats->is_directory = info->mKind == kObjectKindDirectory;
  hdfs_->hdfsFreeFileInfo(info, 1);
  return Status::OK();
}

REGISTER_FILE_SYSTEM("hdfs", HadoopFileSystem);
REGISTER_FILE_SYSTEM("viewfs", HadoopFileSystem);

}  // namespace tensorflow
