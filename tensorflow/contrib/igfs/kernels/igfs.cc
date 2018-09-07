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

const string USER_NAME = "";

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
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  std::string path = TranslateName(file_name);
  CtrlResponse<Optional<OpenReadResponse>> open_read_response = {};
  TF_RETURN_IF_ERROR(client->OpenRead(&open_read_response, path));
  long resource_id = open_read_response.GetRes().Get().stream_id;
  result->reset(new IGFSRandomAccessFile(path, resource_id, client));

  LOG(INFO) << "New random access file completed successfully [file_name="
            << file_name << "]";

  return Status::OK();
}

Status IGFS::NewWritableFile(const std::string &fname,
                             std::unique_ptr<WritableFile> *result) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  std::string path = TranslateName(fname);

  // Check if file exists, and if yes delete it.
  CtrlResponse<ExistsResponse> exists_response = {};
  TF_RETURN_IF_ERROR(client->Exists(&exists_response, path));

  if (exists_response.GetRes().exists) {
    CtrlResponse<DeleteResponse> delResponse = {};
    TF_RETURN_IF_ERROR(client->Delete(&delResponse, path, false));
  }

  CtrlResponse<OpenCreateResponse> open_create_resp = {};
  TF_RETURN_IF_ERROR(client->OpenCreate(&open_create_resp, path));
  long resource_id = open_create_resp.GetRes().stream_id;
  result->reset(new IGFSWritableFile(path, resource_id, client));
  
  return Status::OK();
}

Status IGFS::NewAppendableFile(const std::string &fname,
                               std::unique_ptr<WritableFile> *result) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  // Check if file exists, and if yes delete it.
  CtrlResponse<ExistsResponse> exists_response = {};
  TF_RETURN_IF_ERROR(client->Exists(&exists_response, fname));

  if (exists_response.GetRes().exists) {
    CtrlResponse<DeleteResponse> del_response = {};
    TF_RETURN_IF_ERROR(client->Delete(&del_response, fname, false));
  }

  CtrlResponse<OpenAppendResponse> openAppendResp = {};
  TF_RETURN_IF_ERROR(client->OpenAppend(&openAppendResp, fname));
  long resource_id = openAppendResp.GetRes().stream_id;
  result->reset(new IGFSWritableFile(TranslateName(fname), resource_id, client));

  return Status::OK();
}

Status IGFS::NewReadOnlyMemoryRegionFromFile(
    const std::string &fname, std::unique_ptr<ReadOnlyMemoryRegion> *result) {
  return errors::Unimplemented("IGFS does not support ReadOnlyMemoryRegion");
}

Status IGFS::FileExists(const std::string &fname) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string path = TranslateName(fname);
  CtrlResponse<ExistsResponse> exists_response = {};
  TF_RETURN_IF_ERROR(client->Exists(&exists_response, path));

  if (exists_response.GetRes().exists) {
    return Status::OK();
  }

  return errors::NotFound(path, " not found");
}

std::string MakeRelative(const std::string &a, const std::string &b) {
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
  return string((firstIsShortest ? r.first : r.second),// + 1,
                firstIsShortest ? min.end() : max.end());
}

Status IGFS::GetChildren(const std::string &fname,
                         std::vector<string> *result) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string dir = TranslateName(fname);
  CtrlResponse<ListPathsResponse> list_paths_response = {};
  TF_RETURN_IF_ERROR(client->ListPaths(&list_paths_response, dir));

  *result = vector<string>();
  vector<IGFSPath> entries = list_paths_response.GetRes().entries;

  for (auto &value : entries) {
    result->push_back(MakeRelative(value.path, dir));
  }

  return Status::OK();
}

Status IGFS::GetMatchingPaths(const std::string &pattern,
                              std::vector<string> *results) {
  return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
}

Status IGFS::DeleteFile(const std::string &fname) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string path = TranslateName(fname);
  CtrlResponse<DeleteResponse> del_response = {};
  TF_RETURN_IF_ERROR(client->Delete(&del_response, path, false));
  
  if (!del_response.GetRes().exists) {
    return errors::NotFound(path, " not found");
  }

  return Status::OK();
}

Status IGFS::CreateDir(const std::string &fname) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> hResponse = {};
  TF_RETURN_IF_ERROR(client->Handshake(&hResponse));

  const std::string dir = TranslateName(fname);
  CtrlResponse<MakeDirectoriesResponse> mkdir_response = {};
  TF_RETURN_IF_ERROR(client->MkDir(&mkdir_response, dir));

  if (!mkdir_response.GetRes().successful) {
    return errors::Internal("Error during creating directory");
  }

  return Status::OK();
}

Status IGFS::DeleteDir(const std::string &dir) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string dir_name = TranslateName(dir);
  CtrlResponse<ListFilesResponse> list_files_response = {};
  client->ListFiles(&list_files_response, dir_name);

  if (!list_files_response.GetRes().entries.empty()) {
    return errors::FailedPrecondition("Cannot delete a non-empty directory");
  } else {
    CtrlResponse<DeleteResponse> del_response = {};
    TF_RETURN_IF_ERROR(client->Delete(&del_response, dir_name, true));
  }

  return Status::OK();
}

Status IGFS::GetFileSize(const std::string &fname, uint64 *size) {
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string path = TranslateName(fname);
  CtrlResponse<InfoResponse> info_response = {};
  TF_RETURN_IF_ERROR(client->Info(&info_response, path));
  *size = info_response.GetRes().file_info.length;

  return Status::OK();
}

Status IGFS::RenameFile(const std::string &src, const std::string &target) {
  if (FileExists(target).ok()) {
    DeleteFile(target);
  }

  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string src_path = TranslateName(src);
  const std::string target_path = TranslateName(target);

  CtrlResponse<RenameResponse> renameResp = {};
  TF_RETURN_IF_ERROR(client->Rename(&renameResp, src_path, target_path));

  if (!renameResp.GetRes().IsSuccessful()) {
    return errors::NotFound(src_path, " not found");
  }

  return Status::OK();
}

Status IGFS::Stat(const std::string &fname, FileStatistics *stats) {
  LOG(INFO) << "Stat " << fname;
  shared_ptr<IGFSClient> client(new IGFSClient(host_, port_, fs_name_, USER_NAME));

  CtrlResponse<Optional<HandshakeResponse>> handshake_response = {};
  TF_RETURN_IF_ERROR(client->Handshake(&handshake_response));

  const std::string path = TranslateName(fname);
  CtrlResponse<InfoResponse> info_response = {};
  TF_RETURN_IF_ERROR(client->Info(&info_response, path));

  IGFSFile info = info_response.GetRes().file_info;
  LOG(INFO) << "File Size : " << info.length;
  *stats = FileStatistics(info.length, info.modification_time,
                              (info.flags & 0x1) != 0);

  return Status::OK();
}

}  // namespace tensorflow
