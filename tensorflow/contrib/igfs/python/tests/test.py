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
import tensorflow.contrib.igfs.python.ops.igfs_ops


class FileSystemTest(test.TestCase):

  def setUp(self):
    pass

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
