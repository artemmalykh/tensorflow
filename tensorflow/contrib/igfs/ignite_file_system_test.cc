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

#include "ignite_file_system.h"

using namespace tensorflow;

int main(int argc, char* argv[]) {
  IgniteFileSystem igfs = IgniteFileSystem();

  auto * wf = new unique_ptr<WritableFile>();

  const Status status = igfs.NewWritableFile("/test", wf);

  WritableFile *wFile = wf->get();
  const string str = "KjmsGns!";
  wFile->Append(StringPiece(str));
//  wFile->Close();

//  auto * rf = new unique_ptr<RandomAccessFile>();
//  igfs.NewRandomAccessFile("/test", rf);
//  RandomAccessFile *rFile = rf->get();
//  unique_ptr<StringPiece> res(new StringPiece());
//  char scratch[str.size()] = {};
//  rFile->Read(0, str.size(), res.get(), scratch);
}