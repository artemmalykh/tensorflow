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

#include "ignite_dataset_iterator.h"

#include "ignite_plain_client.h"
#include "ignite_ssl_wrapper.h"
#include "tensorflow/core/platform/logging.h"

#include <time.h>
#include <chrono>

namespace tensorflow {

IgniteDatasetIterator::IgniteDatasetIterator(
    const Params& params, std::string host, int32 port,
    std::string cache_name, bool local, int32 part,
    int32 page_size, std::string username, std::string password,
    std::string certfile, std::string keyfile, std::string cert_password,
    std::vector<int32> schema,
    std::vector<int32> permutation)
    : DatasetIterator<IgniteDataset>(params),
      cache_name(cache_name),
      local(local),
      part(part),
      page_size(page_size),
      username(username),
      password(password),
      schema(schema),
      permutation(permutation),
      remainder(-1),
      cursor_id(-1),
      last_page(false) {
  Client* p_client = new PlainClient(host, port);

  if (certfile.empty())
    client = std::unique_ptr<Client>(p_client);
  else
    client = std::unique_ptr<Client>(new SslWrapper(
        std::unique_ptr<Client>(p_client), certfile, keyfile, cert_password));

  LOG(INFO) << "Ignite Dataset Iterator created";
}

IgniteDatasetIterator::~IgniteDatasetIterator() {
  Status status = CloseConnection();
  if (!status.ok()) LOG(ERROR) << status.ToString();

  LOG(INFO) << "Ignite Dataset Iterator destroyed";
}

Status IgniteDatasetIterator::EstablishConnection() {
  if (!client->IsConnected()) {
    Status status = client->Connect();
    if (!status.ok()) return status;

    status = Handshake();
    if (!status.ok()) {
      Status disconnect_status = client->Disconnect();
      if (!disconnect_status.ok()) LOG(ERROR) << disconnect_status.ToString();

      return status;
    }
  }

  return Status::OK();
}

Status IgniteDatasetIterator::CloseConnection() {
  if (cursor_id != -1 && !last_page) {
    Status conn_status = EstablishConnection();
    if (!conn_status.ok()) return conn_status;

    TF_RETURN_IF_ERROR(client->WriteInt(18));  // Message length
    TF_RETURN_IF_ERROR(
        client->WriteShort(close_connection_opcode));  // Operation code
    TF_RETURN_IF_ERROR(client->WriteLong(0));                // Request ID
    TF_RETURN_IF_ERROR(client->WriteLong(cursor_id));        // Resource ID

    int32_t res_len;
    TF_RETURN_IF_ERROR(client->ReadInt(res_len));
    if (res_len < 12)
      return errors::Internal(
          "Close Resource Response is corrupted");

    int64_t req_id;
    TF_RETURN_IF_ERROR(client->ReadLong(req_id));
    int32_t status;
    TF_RETURN_IF_ERROR(client->ReadInt(status));
    if (status != 0) {
      uint8_t err_msg_header;
      TF_RETURN_IF_ERROR(client->ReadByte(err_msg_header));
      if (err_msg_header == string_val) {
        int32_t err_msg_length;
        TF_RETURN_IF_ERROR(client->ReadInt(err_msg_length));
        uint8_t* err_msg_c = new uint8_t[err_msg_length];
        TF_RETURN_IF_ERROR(client->ReadData(err_msg_c, err_msg_length));
        std::string err_msg((char*)err_msg_c, err_msg_length);
        delete[] err_msg_c;

        return errors::Internal("Close Resource Error [status=",
                                            status, ", message=", err_msg, "]");
      }
      return errors::Internal("Close Resource Error [status=",
                                          status, "]");
    }

    LOG(INFO) << "Query Cursor " << cursor_id << " is closed";

    cursor_id = -1;

    return client->Disconnect();
  } else {
    LOG(INFO) << "Query Cursor " << cursor_id << " is already closed";
  }

  return client->IsConnected() ? client->Disconnect()
                               : Status::OK();
}

Status IgniteDatasetIterator::GetNextInternal(
    IteratorContext* ctx,
    std::vector<Tensor>* out_tensors, bool* end_of_sequence) {
  if (remainder == 0 && last_page) {
    LOG(INFO) << "Query Cursor " << cursor_id << " is closed";

    cursor_id = -1;
    *end_of_sequence = true;
    return Status::OK();
  } else {
    Status status = EstablishConnection();
    if (!status.ok()) return status;

    if (remainder == -1 || remainder == 0) {
      Status status =
          remainder == -1 ? ScanQuery() : LoadNextPage();
      if (!status.ok()) return status;
    }

    uint8_t* initial_ptr = ptr;
    std::vector<int32_t> types;
    std::vector<Tensor> tensors;

    status = parser.Parse(&ptr, &tensors, &types);  // Parse key
    if (!status.ok()) return status;

    status = parser.Parse(&ptr, &tensors, &types);  // Parse val
    if (!status.ok()) return status;

    remainder -= (ptr - initial_ptr);

    out_tensors->resize(tensors.size());
    for (int32_t i = 0; i < tensors.size(); i++)
      (*out_tensors)[permutation[i]] = std::move(tensors[i]);

    *end_of_sequence = false;
    return Status::OK();
  }

  *end_of_sequence = true;
  return Status::OK();
}

Status IgniteDatasetIterator::SaveInternal(
    IteratorStateWriter* writer) {
  return errors::Unimplemented(
      "Iterator for IgniteDataset does not support 'SaveInternal'");
}

Status IgniteDatasetIterator::RestoreInternal(
    IteratorContext* ctx, IteratorStateReader* reader) {
  return errors::Unimplemented(
      "Iterator for IgniteDataset does not support 'RestoreInternal')");
}

Status IgniteDatasetIterator::Handshake() {
  int32_t msg_len = 8;

  if (username.empty())
    msg_len += 1;
  else
    msg_len += 5 + username.length();

  if (password.empty())
    msg_len += 1;
  else
    msg_len += 5 + password.length();

  TF_RETURN_IF_ERROR(client->WriteInt(msg_len));
  TF_RETURN_IF_ERROR(client->WriteByte(1));
  TF_RETURN_IF_ERROR(client->WriteShort(protocol_major_version));
  TF_RETURN_IF_ERROR(client->WriteShort(protocol_minor_version));
  TF_RETURN_IF_ERROR(client->WriteShort(protocol_patch_version));
  TF_RETURN_IF_ERROR(client->WriteByte(2));
  if (username.empty()) {
    TF_RETURN_IF_ERROR(client->WriteByte(null_val));
  } else {
    TF_RETURN_IF_ERROR(client->WriteByte(string_val));
    TF_RETURN_IF_ERROR(client->WriteInt(username.length()));
    TF_RETURN_IF_ERROR(
        client->WriteData((uint8_t*)username.c_str(), username.length()));
  }

  if (password.empty()) {
    TF_RETURN_IF_ERROR(client->WriteByte(null_val));
  } else {
    TF_RETURN_IF_ERROR(client->WriteByte(string_val));
    TF_RETURN_IF_ERROR(client->WriteInt(password.length()));
    TF_RETURN_IF_ERROR(
        client->WriteData((uint8_t*)password.c_str(), password.length()));
  }

  int32_t handshake_res_len;
  TF_RETURN_IF_ERROR(client->ReadInt(handshake_res_len));
  uint8_t handshake_res;
  TF_RETURN_IF_ERROR(client->ReadByte(handshake_res));

  LOG(INFO) << "Handshake length " << handshake_res_len << ", res "
            << (int16_t)handshake_res;

  if (handshake_res != 1) {
    int16_t serv_ver_major;
    TF_RETURN_IF_ERROR(client->ReadShort(serv_ver_major));
    int16_t serv_ver_minor;
    TF_RETURN_IF_ERROR(client->ReadShort(serv_ver_minor));
    int16_t serv_ver_patch;
    TF_RETURN_IF_ERROR(client->ReadShort(serv_ver_patch));
    uint8_t header;
    TF_RETURN_IF_ERROR(client->ReadByte(header));

    if (header == string_val) {
      int32_t length;
      TF_RETURN_IF_ERROR(client->ReadInt(length));
      uint8_t* err_msg_c = new uint8_t[length];
      TF_RETURN_IF_ERROR(client->ReadData(err_msg_c, length));
      std::string err_msg((char*)err_msg_c, length);
      delete[] err_msg_c;

      return errors::Internal(
          "Handshake Error [result=", handshake_res, ", version=",
          serv_ver_major, ".", serv_ver_minor, ".", serv_ver_patch,
          ", message='", err_msg, "']");
    } else if (header == null_val) {
      return errors::Internal(
          "Handshake Error [result=", handshake_res, ", version=",
          serv_ver_major, ".", serv_ver_minor, ".", serv_ver_patch, "]");
    } else {
      return errors::Internal(
          "Handshake Error [result=", handshake_res, ", version=",
          serv_ver_major, ".", serv_ver_minor, ".", serv_ver_patch, "]");
    }
  }

  return Status::OK();
}

Status IgniteDatasetIterator::ScanQuery() {
  TF_RETURN_IF_ERROR(client->WriteInt(25));                        // Message length
  TF_RETURN_IF_ERROR(client->WriteShort(scan_query_opcode));       // Operation code
  TF_RETURN_IF_ERROR(client->WriteLong(0));                        // Request ID
  TF_RETURN_IF_ERROR(client->WriteInt(JavaHashCode(cache_name)));  // Cache name
  TF_RETURN_IF_ERROR(client->WriteByte(0));                        // Flags
  TF_RETURN_IF_ERROR(client->WriteByte(null_val));                 // Filter object
  TF_RETURN_IF_ERROR(client->WriteInt(page_size));                 // Cursor page size
  TF_RETURN_IF_ERROR(client->WriteInt(part));    // Partition to query
  TF_RETURN_IF_ERROR(client->WriteByte(local));  // Local flag

  int64_t wait_start = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

  int32_t res_len;
  TF_RETURN_IF_ERROR(client->ReadInt(res_len));

  int64_t wait_stop = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  LOG(INFO) << "Scan Query waited " << (wait_stop - wait_start) << " ms";

  if (res_len < 12)
    return errors::Internal("Scan Query Response is corrupted");

  int64_t req_id;
  TF_RETURN_IF_ERROR(client->ReadLong(req_id));

  int32_t status;
  TF_RETURN_IF_ERROR(client->ReadInt(status));

  if (status != 0) {
    uint8_t err_msg_header;
    TF_RETURN_IF_ERROR(client->ReadByte(err_msg_header));

    if (err_msg_header == string_val) {
      int32_t err_msg_length;
      TF_RETURN_IF_ERROR(client->ReadInt(err_msg_length));

      uint8_t* err_msg_c = new uint8_t[err_msg_length];
      TF_RETURN_IF_ERROR(client->ReadData(err_msg_c, err_msg_length));
      std::string err_msg((char*)err_msg_c, err_msg_length);
      delete[] err_msg_c;

      return errors::Internal("Scan Query Error [status=", status,
                                          ", message=", err_msg, "]");
    }
    return errors::Internal("Scan Query Error [status=", status,
                                        "]");
  }

  TF_RETURN_IF_ERROR(client->ReadLong(cursor_id));

  LOG(INFO) << "Query Cursor " << cursor_id << " is opened";

  int32_t row_cnt;
  TF_RETURN_IF_ERROR(client->ReadInt(row_cnt));

  remainder = res_len - 25;
  page = std::unique_ptr<uint8_t>(new uint8_t[remainder]);
  ptr = page.get();

  int64_t start = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

  TF_RETURN_IF_ERROR(client->ReadData(ptr, remainder));

  int64_t stop = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  ;

  double size_in_mb = 1.0 * remainder / 1024 / 1024;
  double time_in_s = 1.0 * (stop - start) / 1000;
  LOG(INFO) << "Page size " << size_in_mb << " Mb, time " << time_in_s * 1000
            << " ms download speed " << size_in_mb / time_in_s << " Mb/sec";

  uint8_t last_page_b;
  TF_RETURN_IF_ERROR(client->ReadByte(last_page_b));

  last_page = !last_page_b;

  return Status::OK();
}

Status IgniteDatasetIterator::LoadNextPage() {
  TF_RETURN_IF_ERROR(client->WriteInt(18));                       // Message length
  TF_RETURN_IF_ERROR(client->WriteShort(load_next_page_opcode));  // Operation code
  TF_RETURN_IF_ERROR(client->WriteLong(0));                       // Request ID
  TF_RETURN_IF_ERROR(client->WriteLong(cursor_id));               // Cursor ID

  int64_t wait_start = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

  int32_t res_len;
  TF_RETURN_IF_ERROR(client->ReadInt(res_len));

  int64_t wait_stop = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  LOG(INFO) << "Load Next Page waited " << (wait_stop - wait_start) << " ms";

  if (res_len < 12)
    return errors::Internal("Load Next Page Response is corrupted");

  int64_t req_id;
  TF_RETURN_IF_ERROR(client->ReadLong(req_id));

  int32_t status;
  TF_RETURN_IF_ERROR(client->ReadInt(status));

  if (status != 0) {
    uint8_t err_msg_header;
    TF_RETURN_IF_ERROR(client->ReadByte(err_msg_header));

    if (err_msg_header == string_val) {
      int32_t err_msg_length;
      TF_RETURN_IF_ERROR(client->ReadInt(err_msg_length));

      uint8_t* err_msg_c = new uint8_t[err_msg_length];
      TF_RETURN_IF_ERROR(client->ReadData(err_msg_c, err_msg_length));
      std::string err_msg((char*)err_msg_c, err_msg_length);
      delete[] err_msg_c;

      return errors::Internal("Load Next Page Error [status=",
                                          status, ", message=", err_msg, "]");
    }
    return errors::Internal("Load Next Page Error [status=", status,
                                        "]");
  }

  int32_t row_cnt;
  TF_RETURN_IF_ERROR(client->ReadInt(row_cnt));

  remainder = res_len - 17;
  page = std::unique_ptr<uint8_t>(new uint8_t[remainder]);
  ptr = page.get();

  int64_t start = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

  TF_RETURN_IF_ERROR(client->ReadData(ptr, remainder));

  int64_t stop = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
  ;

  double size_in_mb = 1.0 * remainder / 1024 / 1024;
  double time_in_s = 1.0 * (stop - start) / 1000;
  LOG(INFO) << "Page size " << size_in_mb << " Mb, time " << time_in_s * 1000
            << " ms download speed " << size_in_mb / time_in_s << " Mb/sec";

  uint8_t last_page_b;
  TF_RETURN_IF_ERROR(client->ReadByte(last_page_b));

  last_page = !last_page_b;

  return Status::OK();
}

int32_t IgniteDatasetIterator::JavaHashCode(std::string str) {
  int32_t h = 0;
  for (char& c : str) {
    h = 31 * h + c;
  }
  return h;
}

}  // namespace tensorflow
