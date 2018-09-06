# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations under
# the License.
# ==============================================================================
"""Tests for IGFS."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.igfs.python.ops.igfs_ops
from tensorflow.python.platform import test

class IGFSTest(test.TestCase):
  """The Apache Ignite servers have to setup before the test and tear down
     after the test manually. The docker engine has to be installed.

     To setup Apache Ignite servers:
     $ bash start_ignite.sh

     To tear down Apache Ignite servers:
     $ bash stop_ignite.sh
  """
  
  def test_create_file(self):
    # Setup and check preconditions.
    file_name = "igfs:///test_create_file/1"
    self.assertFalse(tf.gfile.Exists(file_name))
    # Create file.
    with tf.gfile.Open(file_name, mode='w') as w:
      w.write("")
    # Check that file was created.
    self.assertTrue(tf.gfile.Exists(file_name))

  def test_write_read_file(self):
    # Setup and check preconditions.
    file_name = "igfs:///test_write_read_file/1"
    rows = 10000
    self.assertFalse(tf.gfile.Exists(file_name))
    # Write data.
    with tf.gfile.Open(file_name, mode='w') as w:
      for i in range(rows):
        w.write("This is row\n")
    # Read data.
    with tf.gfile.Open(file_name, mode='r') as r:
      lines = r.readlines()
    # Check that data is equal.
    self.assertEqual(rows, len(lines))
    for i in range(rows):
      self.assertEqual("This is row\n", lines[i])

  def test_delete_recursively(self):
    # Setup and check preconditions.
    dir_name = "igfs:///test_delete_recursively/"
    file_name = "igfs:///test_delete_recursively/1"
    self.assertFalse(tf.gfile.Exists(dir_name))
    self.assertFalse(tf.gfile.Exists(file_name))
    tf.gfile.MkDir(dir_name)
    with tf.gfile.Open(file_name, mode='w') as w:
      w.write("")
    self.assertTrue(tf.gfile.Exists(dir_name))
    self.assertTrue(tf.gfile.Exists(file_name))
    # Delete directory recursively.
    tf.gfile.DeleteRecursively(dir_name)
    # Check that directory was deleted.
    self.assertFalse(tf.gfile.Exists(dir_name))
    self.assertFalse(tf.gfile.Exists(file_name))

  def test_copy(self):
    # Setup and check preconditions.
    src_file_name = "igfs:///test_copy/1"
    dst_file_name = "igfs:///test_copy/2"
    self.assertFalse(tf.gfile.Exists(src_file_name))
    self.assertFalse(tf.gfile.Exists(dst_file_name))
    with tf.gfile.Open(src_file_name, mode='w') as w:
      w.write("42")
    self.assertTrue(tf.gfile.Exists(src_file_name))
    self.assertFalse(tf.gfile.Exists(dst_file_name))
    # Copy file.
    tf.gfile.Copy(src_file_name, dst_file_name)
    # Check that files are identical.
    self.assertTrue(tf.gfile.Exists(src_file_name))
    self.assertTrue(tf.gfile.Exists(dst_file_name))
    with tf.gfile.Open(dst_file_name, mode='r') as r:
      data = r.read()
    self.assertEqual("42", data)

  def test_is_directory(self):
    # Setup and check preconditions.
    dir_name = "igfs:///test_is_directory/1"
    file_name = "igfs:///test_is_directory/2"
    with tf.gfile.Open(file_name, mode='w') as w:
      w.write("")
    tf.gfile.MkDir(dir_name)
    # Check that directory is a directory.
    self.assertTrue(tf.gfile.IsDirectory(dir_name))
    # Check that file is not a directory.
    self.assertFalse(tf.gfile.IsDirectory(file_name))

  def test_list_directory(self):
    # Setup and check preconditions.
    dir_name = "igfs:///test_list_directory/"
    file_names = [
      "igfs:///test_list_directory/1",
      "igfs:///test_list_directory/2/3"
    ]
    ch_dir_names = [
      "igfs:///test_list_directory/4",
    ]
    for file_name in file_names:
      with tf.gfile.Open(file_name, mode='w') as w:
        w.write("")
    for ch_dir_name in ch_dir_names:
      tf.gfile.MkDir(ch_dir_name)
    ls_expected_result = file_names + ch_dir_names
    # Get list of files in directory.
    ls_result = tf.gfile.ListDirectory(dir_name)
    # Check that list of files is correct.
    self.assertEqual(len(ls_expected_result), len(ls_result))
    for e in ['1', '2', '4']:
      self.assertTrue(e in ls_result)

  def test_make_dirs(self):
    # Setup and check preconditions.
    dir_name = "igfs:///test_make_dirs/"
    self.assertFalse(tf.gfile.Exists(dir_name))
    # Make directory.
    tf.gfile.MkDir(dir_name)
    # Check that directory was created.
    self.assertTrue(tf.gfile.Exists(dir_name))

  def test_remove(self):
    # Setup and check preconditions.
    file_name = "igfs:///test_remove/1"
    self.assertFalse(tf.gfile.Exists(file_name))
    with tf.gfile.Open(file_name, mode='w') as w:
      w.write("");
    self.assertTrue(tf.gfile.Exists(file_name))
    # Remove file.
    tf.gfile.Remove(file_name)
    # Check that file was removed.
    self.assertFalse(tf.gfile.Exists(file_name))

  def test_rename_file(self):
    # Setup and check preconditions.
    src_file_name = "igfs:///test_rename_file/1"
    dst_file_name = "igfs:///test_rename_file/2"
    with tf.gfile.Open(src_file_name, mode='w') as w:
      w.write("42")
    self.assertTrue(tf.gfile.Exists(src_file_name))
    # Rename file.
    tf.gfile.Rename(src_file_name, dst_file_name)
    # Check that only new name of file is available.
    self.assertFalse(tf.gfile.Exists(src_file_name))
    self.assertTrue(tf.gfile.Exists(dst_file_name))
    with tf.gfile.Open(dst_file_name, mode='r') as r:
      data = r.read()
    self.assertEqual("42", data)

  def test_rename_dir(self):
    # Setup and check preconditions.
    src_dir_name = "igfs:///test_rename_dir/1"
    dst_dir_name = "igfs:///test_rename_dir/2"
    tf.gfile.MkDir(src_dir_name)
    # Rename directory.
    tf.gfile.Rename(src_dir_name, dst_dir_name)
    # Check that only new name of directory is available.
    self.assertFalse(tf.gfile.Exists(src_dir_name))
    self.assertTrue(tf.gfile.Exists(dst_dir_name))
    self.assertTrue(tf.gfile.IsDirectory(dst_dir_name))

if __name__ == "__main__":
  test.main()
