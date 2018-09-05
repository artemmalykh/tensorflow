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

#include "igfs.h"
#include "igfs_client.h"
#include "igfs_random_access_file.h"
#include "igfs_writable_file.h"

namespace tensorflow {

std::string GetEnvOrElse(const std::string &env, std::string default_value) {
  const char *env_c_str = env.c_str();
  return getenv(env_c_str) != nullptr ? getenv(env_c_str) : default_value;
}

IGFS::IGFS()
    : host_(GetEnvOrElse("IGFS_HOST", "localhost")),
      port_(atoi(GetEnvOrElse("IGFS_PORT", "10500").c_str())),
      fs_name_(GetEnvOrElse("IGFS_FS_NAME", "myFileSystem")) {
  LOG(INFO) << "IGFS created [host=" << host_ << ", port=" << port_
            << ", fs_name=" << fs_name_ << "]";
};

IGFS::~IGFS() {
  LOG(INFO) << "IGFS destroyed [host=" << host_ << ", port=" << port_
            << ", fs_name=" << fs_name_ << "]";
};

std::string IGFS::TranslateName(const std::string &name) const {
  StringPiece scheme, namenode, path;
  io::ParseURI(name, &scheme, &namenode, &path);
  return path.ToString();
}

Status IGFS::NewRandomAccessFile(const std::string &file_name,
                                 std::unique_ptr<RandomAccessFile> *result) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));
  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string path = TranslateName(file_name);

    ControlResponse<Optional<OpenReadResponse>> open_read_response = {};
    TF_RETURN_IF_ERROR(client->OpenRead(&open_read_response, "", path));

    if (open_read_response.IsOk()) {
      long resource_id = open_read_response.GetRes().Get().GetStreamId();
      result->reset(new IGFSRandomAccessFile(path, resource_id, client));
    } else {
      return errors::Internal("Error while trying to open for reading");
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  LOG(INFO) << "New random access file completed successfully [file_name="
            << file_name << "]";

  return Status::OK();
}

Status IGFS::NewWritableFile(const std::string &fname,
                             std::unique_ptr<WritableFile> *result) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    std::string path = TranslateName(fname);

    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> exists_response = {};
    TF_RETURN_IF_ERROR(client->Exists(&exists_response, path));

    if (exists_response.IsOk()) {
      if (exists_response.GetRes().Exists()) {
        ControlResponse<DeleteResponse> delResponse = {};
        TF_RETURN_IF_ERROR(client->Delete(&delResponse, path, false));

        if (!delResponse.IsOk()) {
          return errors::Internal("Error trying to delete existing file");
        }
      }
    } else {
      return errors::Internal("Error trying to know if file exists");
    }

    ControlResponse<OpenCreateResponse> open_create_resp = {};
    TF_RETURN_IF_ERROR(client->OpenCreate(&open_create_resp, path));

    if (open_create_resp.IsOk()) {
      long resource_id = open_create_resp.GetRes().GetStreamId();

      result->reset(new IGFSWritableFile(path, resource_id, client));
    } else {
      return errors::Internal("Error during open/create request");
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::NewAppendableFile(const std::string &fname,
                               std::unique_ptr<WritableFile> *result) {
  LOG(INFO) << "New appendable file " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    // Check if file exists, and if yes delete it.
    ControlResponse<ExistsResponse> exists_response = {};
    TF_RETURN_IF_ERROR(client->Exists(&exists_response, fname));

    if (exists_response.IsOk()) {
      if (exists_response.GetRes().Exists()) {
        ControlResponse<DeleteResponse> del_response = {};
        TF_RETURN_IF_ERROR(client->Delete(&del_response, fname, false));

        if (!del_response.IsOk()) {
          return errors::Internal("Error trying to delete existing file");
        }
      }
    } else {
      return errors::Internal("Error trying to know if file exists");
    }

    ControlResponse<OpenAppendResponse> openAppendResp = {};
    TF_RETURN_IF_ERROR(client->OpenAppend(&openAppendResp, "", fname));

    if (openAppendResp.IsOk()) {
      long resource_id = openAppendResp.GetRes().GetStreamId();

      result->reset(
          new IGFSWritableFile(TranslateName(fname), resource_id, client));
    } else {
      return errors::Internal("Error");
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::NewReadOnlyMemoryRegionFromFile(
    const std::string &fname, std::unique_ptr<ReadOnlyMemoryRegion> *result) {
  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IGFS::FileExists(const std::string &fname) {
  LOG(INFO) << "File exists " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string path = TranslateName(fname);
    ControlResponse<ExistsResponse> exists_response = {};
    TF_RETURN_IF_ERROR(client->Exists(&exists_response, path));

    if (exists_response.IsOk()) {
      if (exists_response.GetRes().Exists()) {
        return Status::OK();
      }

      return errors::NotFound(path, " not found");
    } else {
      return errors::Internal("Error");
    }
  } else {
    return errors::Internal("Handshake failed");
  }
}

std::string makeRelative(const std::string &a, const std::string &b) {
  std::string max = a;
  std::string min = b;
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

Status IGFS::GetChildren(const std::string &fname,
                         std::vector<string> *result) {
  LOG(INFO) << "Get children " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string dir = TranslateName(fname);

    ControlResponse<ListPathsResponse> list_paths_response = {};
    TF_RETURN_IF_ERROR(client->ListPaths(&list_paths_response, dir));

    if (!list_paths_response.IsOk()) {
      return errors::Internal("Error");
    }

    *result = vector<string>();
    vector<IgnitePath> entries = list_paths_response.GetRes().getEntries();

    for (auto &value : entries) {
      result->push_back(makeRelative(value.getPath(), dir));
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::GetMatchingPaths(const std::string &pattern,
                              std::vector<string> *results) {
  return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
}

Status IGFS::DeleteFile(const std::string &fname) {
  LOG(INFO) << "Delete file " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string path = TranslateName(fname);

    ControlResponse<DeleteResponse> del_response = {};
    TF_RETURN_IF_ERROR(client->Delete(&del_response, path, false));

    if (!del_response.IsOk()) {
      return errors::Internal("Error");
    }

    if (!del_response.GetRes().exists()) {
      return errors::NotFound(path, " not found");
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::CreateDir(const std::string &fname) {
  LOG(INFO) << "Get dir " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->Handshake(&hResponse));

  if (hResponse.IsOk()) {
    const std::string dir = TranslateName(fname);

    ControlResponse<MakeDirectoriesResponse> mkdir_response = {};
    TF_RETURN_IF_ERROR(client->MkDir(&mkdir_response, dir));

    if (!(mkdir_response.IsOk() && mkdir_response.GetRes().IsSuccessful())) {
      return errors::Internal("Error during creating directory");
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::DeleteDir(const std::string &dir) {
  LOG(INFO) << "Delete dir " << dir;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    ControlResponse<ListFilesResponse> list_files_response = {};
    client->ListFiles(&list_files_response, dir);

    if (!list_files_response.IsOk()) {
      return errors::Internal("Error");
    } else {
      if (!list_files_response.GetRes().getEntries().empty()) {
        return errors::FailedPrecondition(
            "Cannot delete a non-empty directory");
      } else {
        ControlResponse<DeleteResponse> del_response = {};
        TF_RETURN_IF_ERROR(client->Delete(&del_response, dir, true));

        if (!del_response.IsOk()) {
          return errors::Internal("Error while trying to delete directory");
        }
      }
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::GetFileSize(const std::string &fname, uint64 *size) {
  LOG(INFO) << "Get File size " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string path = TranslateName(fname);
    ControlResponse<InfoResponse> info_response = {};
    TF_RETURN_IF_ERROR(client->Info(&info_response, path));

    if (!info_response.IsOk()) {
      return errors::Internal("Error while getting info");
    } else {
      *size = info_response.GetRes().getFileInfo().GetFileSize();
    }

  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::RenameFile(const std::string &src, const std::string &target) {
  LOG(INFO) << "Rename file " << src;
  if (FileExists(target).ok()) {
    DeleteFile(target);
  }

  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string src_path = TranslateName(src);
    const std::string target_path = TranslateName(target);

    ControlResponse<RenameResponse> renameResp = {};
    TF_RETURN_IF_ERROR(client->Rename(&renameResp, src_path, target_path));

    if (!renameResp.IsOk()) {
      return errors::Internal("Error while renaming");
    }

    if (!renameResp.GetRes().IsSuccessful()) {
      return errors::NotFound(src_path, " not found");
    }
  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

Status IGFS::Stat(const std::string &fname, FileStatistics *stats) {
  LOG(INFO) << "Stat " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_));

  ControlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  if (handshake_response.IsOk()) {
    const std::string path = TranslateName(fname);
    ControlResponse<InfoResponse> info_response = {};
    TF_RETURN_IF_ERROR(client->Info(&info_response, path));

    if (!info_response.IsOk()) {
      return errors::Internal("Error while getting info");
    } else {
      IgfsFile info = info_response.GetRes().getFileInfo();

      *stats = FileStatistics(info.GetFileSize(), info.GetModificationTime(),
                              (info.GetFlags() & 0x1) != 0);
    }

  } else {
    return errors::Internal("Handshake failed");
  }

  return Status::OK();
}

}  // namespace tensorflow
