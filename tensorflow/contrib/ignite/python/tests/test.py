"""Tests for file_system."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from tensorflow.python.framework import dtypes
from tensorflow.python.framework import load_library
from tensorflow.python.ops import data_flow_ops
from tensorflow.python.ops import io_ops
from tensorflow.python.platform import resource_loader
from tensorflow.python.platform import test
from tensorflow.python.util import compat
import tensorflow as tf

from tensorflow.contrib.ignite import IgniteDataset


class FileSystemTest(test.TestCase):

  def setUp(self):
    # TODO: For some reason library does not load without absolute path.
    file_system_library = os.path.join(resource_loader.get_data_files_path(), "/usr/local/lib/python3.5/dist-packages/tensorflow/contrib/ignite_file_system.so")
    print(file_system_library)
    load_library.load_file_system_library(file_system_library)

  def testBasic(self):
    if (tf.gfile.Exists("igfs:///path/")) :
      print("Directory already exists")
    else :
      print("Directory does not exist.")
      tf.gfile.MkDir("igfs:///path/")
      print("Created.")
      
    with tf.gfile.Open("igfs:///path/file.txt", mode='w') as w:
      w.write("hi")
    with tf.gfile.Open("igfs:///path/file.txt", mode='r') as r:
      content = r.read()
      self.assertEqual(content, "hi") 

if __name__ == "__main__":
  test.main()
