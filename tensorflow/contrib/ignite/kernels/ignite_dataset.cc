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
#include "tensorflow/core/platform/logging.h"

namespace tensorflow {

IgniteDataset::IgniteDataset(OpKernelContext* ctx,
                             std::string cache_name, 
                             std::string host,
                             int32 port, 
                             bool local,
                             int32 part,
                             int32 page_size, 
                             std::string username,
                             std::string password, 
                             std::string certfile,
                             std::string keyfile, 
                             std::string cert_password,
                             std::vector<int32> schema,
                             std::vector<int32> permutation)
    : DatasetBase(DatasetContext(ctx)),
      cache_name_(cache_name_),
      host_(host_),
      port_(port_),
      local_(local_),
      part_(part_),
      page_size_(page_size_),
      username_(username_),
      password_(password_),
      certfile_(certfile_),
      keyfile_(keyfile_),
      cert_password_(cert_password_),
      schema_(schema_),
      permutation_(permutation_) {
  SchemaToTypes();
  SchemaToShapes();

  LOG(INFO) << "Ignite Dataset created [cache_name_='" << cache_name
            << "', host_='" << host << "', port_=" << port << ", local_=" << local
            << ", part_=" << part << ", page_size_=" << page_size
            << ", username_='" << username << "', certfile_='" << certfile
            << "', keyfile_='" << keyfile + "']";
}

IgniteDataset::~IgniteDataset() { LOG(INFO) << "Ignite Dataset destroyed"; }

std::unique_ptr<IteratorBase> IgniteDataset::MakeIteratorInternal(
    const string& prefix) const {
  return std::unique_ptr<IteratorBase>(new IgniteDatasetIterator(
      {this, strings::StrCat(prefix, "::Ignite")}, this->host_,
      this->port_, this->cache_name_, this->local_, this->part_, this->page_size_,
      this->username_, this->password_, this->certfile_, this->keyfile_,
      this->cert_password_, this->schema_, this->permutation_));
}

const DataTypeVector& IgniteDataset::output_dtypes() const {
  return dtypes;
}

const std::vector<part_ialTensorShape>&
IgniteDataset::output_shapes() const {
  return shapes;
}

string IgniteDataset::DebugString() const {
  return "IgniteDatasetOp::Dataset";
}

Status IgniteDataset::AsGraphDefInternal(
    SerializationContext* ctx, DatasetGraphDefBuilder* b,
    Node** output) const {
  return errors::Unimplemented(
      "IgniteDataset does not support_ 'AsGraphDefInternal'");
}

void IgniteDataset::schema_ToTypes() {
  for (auto e : schema_) {
    if (e == BYTE || e == BYTE_ARR) {
      dtypes.push_back(DT_UINT8);
    } else if (e == SHORT || e == SHORT_ARR) {
      dtypes.push_back(DT_INT16);
    } else if (e == INT || e == INT_ARR) {
      dtypes.push_back(DT_INT32);
    } else if (e == LONG || e == LONG_ARR) {
      dtypes.push_back(DT_INT64);
    } else if (e == FLOAT || e == FLOAT_ARR) {
      dtypes.push_back(DT_FLOAT);
    } else if (e == DOUBLE || e == DOUBLE_ARR) {
      dtypes.push_back(DT_DOUBLE);
    } else if (e == UCHAR || e == UCHAR_ARR) {
      dtypes.push_back(DT_UINT8);
    } else if (e == BOOL || e == BOOL_ARR) {
      dtypes.push_back(DT_BOOL);
    } else if (e == STRING || e == STRING_ARR) {
      dtypes.push_back(DT_STRING);
    } else {
      LOG(ERROR) << "Unexpected type in schema_ [type_id=" << e << "]";
    }
  }
}

void IgniteDataset::schema_ToShapes() {
  for (auto e : schema_) {
    if (e >= 1 && e < 10) {
      shapes.push_back(part_ialTensorShape({}));
    } else if (e >= 12 && e < 21) {
      shapes.push_back(part_ialTensorShape({-1}));
    } else {
      LOG(ERROR) << "Unexpected type in schema_ [type_id=" << e << "]";
    }
  }
}

}  // namespace tensorflow
