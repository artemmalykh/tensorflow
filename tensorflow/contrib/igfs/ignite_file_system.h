#ifndef TF_CONTRIB_IGNITE_FILE_SYSTEM_H
#define TF_CONTRIB_IGNITE_FILE_SYSTEM_H

#include "tensorflow/core/platform/env.h"
#include <tensorflow/core/platform/file_system.h>
#include "utils.h"

namespace tensorflow {

using namespace std;

class IgniteFileSystem : public FileSystem {
 public:
  IgniteFileSystem();
  ~IgniteFileSystem();

  Status NewRandomAccessFile(
      const string& fname, std::unique_ptr<RandomAccessFile>* result) override;

  Status NewWritableFile(const string& fname,
                         std::unique_ptr<WritableFile>* result) override;

  Status NewAppendableFile(const string& fname,
                           std::unique_ptr<WritableFile>* result) override;

  Status NewReadOnlyMemoryRegionFromFile(
      const string& fname,
      std::unique_ptr<ReadOnlyMemoryRegion>* result) override;

  Status FileExists(const string& fname) override;

  Status GetChildren(const string& dir, std::vector<string>* result) override;

  Status GetMatchingPaths(const string& pattern,
                          std::vector<string>* results) override;

  Status DeleteFile(const string& fname) override;

  Status CreateDir(const string& name) override;

  Status DeleteDir(const string& name) override;

  Status GetFileSize(const string& fname, uint64* size) override;

  Status RenameFile(const string& src, const string& target) override;

  Status Stat(const string& fname, FileStatistics* stat) override;

  string TranslateName(const string& name) const override;

 private:
  Status Connect(StringPiece fname);
};

}  // namespace tensorflow


#endif //TF_CMAKE_IGNITE_FILE_SYSTEM_H
