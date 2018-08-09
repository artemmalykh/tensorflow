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

#include <tensorflow/core/lib/core/status_test_util.h>
#include "tensorflow/core/lib/gtl/stl_util.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/str_util.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/test.h"
#include "ignite_file_system.h"

namespace tensorflow {
namespace {

class IgniteFileSystemTest : public ::testing::Test {
 protected:
  IgniteFileSystemTest() {}

  string TmpDir(const string &path) {
    char *test_dir = getenv("IGNITE_TEST_TMPDIR");

    if (test_dir != nullptr) {
      return io::JoinPath(string(test_dir), path);
    } else {
      return "file://" + io::JoinPath(testing::TmpDir(), path);
    }
  }

  Status WriteString(const string &fname, const string &content) {
    std::unique_ptr<WritableFile> writer;
    TF_RETURN_IF_ERROR(igfs.NewWritableFile(fname, &writer));
    TF_RETURN_IF_ERROR(writer->Append(content));
    TF_RETURN_IF_ERROR(writer->Close());
    return Status::OK();
  }

  Status ReadAll(const string& fname, string* content) {
    std::unique_ptr<RandomAccessFile> reader;
    TF_RETURN_IF_ERROR(igfs.NewRandomAccessFile(fname, &reader));

    uint64 file_size = 0;
    TF_RETURN_IF_ERROR(igfs.GetFileSize(fname, &file_size));

    content->resize(file_size);
    StringPiece result;
    TF_RETURN_IF_ERROR(
        reader->Read(0, file_size, &result, gtl::string_as_array(content)));
    if (file_size != result.size()) {
      return errors::DataLoss("expected ", file_size, " got ", result.size(),
                              " bytes");
    }
    return Status::OK();
  }

  IgniteFileSystem igfs;
};

TEST_F(IgniteFileSystemTest, RandomAccessFile) {
  const string fname = TmpDir("RandomAccessFile");
  const string content = "abcdefghijklmn";
  TF_ASSERT_OK(WriteString(fname, content));

  std::unique_ptr<RandomAccessFile> reader;
  TF_EXPECT_OK(igfs.NewRandomAccessFile(fname, &reader));

  string got;
  got.resize(content.size());
  StringPiece result;
  TF_EXPECT_OK(
      reader->Read(0, content.size(), &result, gtl::string_as_array(&got)));
  EXPECT_EQ(content.size(), result.size());
  EXPECT_EQ(content, result);

  got.clear();
  got.resize(4);
  TF_EXPECT_OK(reader->Read(2, 4, &result, gtl::string_as_array(&got)));
  EXPECT_EQ(4, result.size());
  EXPECT_EQ(content.substr(2, 4), result);
}

}
}

using namespace tensorflow;

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//int main(int argc, char *argv[]) {
//  IgniteFileSystem igfs = IgniteFileSystem();
//
//  auto *wf = new unique_ptr<WritableFile>();
//
//  const Status status = igfs.NewWritableFile("/test", wf);
//
//  WritableFile *wFile = wf->get();
//  const string str = "KjmsGns!";
//  wFile->Append(StringPiece(str));
//  wFile->Close();
//
//  auto *rf = new unique_ptr<RandomAccessFile>();
//  igfs.NewRandomAccessFile("/test", rf);
//  RandomAccessFile *rFile = rf->get();
//  unique_ptr<StringPiece> res(new StringPiece());
//  char scratch[str.size()] = {};
//  rFile->Read(0, str.size(), res.get(), scratch);
//
//  for (int i = 0; i < str.size(); i++) {
//    assert (scratch[i] == str.c_str()[i]);
//  }
//}