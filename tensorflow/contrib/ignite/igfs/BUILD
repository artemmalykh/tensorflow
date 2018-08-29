# Description:
# Ignite file system implementation.

package(
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])  # Apache 2.0

load(
    "//tensorflow:tensorflow.bzl",
    "tf_cc_test",
)

cc_library(
    name = "ignite_file_system",
    srcs = ["ignite_file_system.cc"],
    hdrs = ["client.h", "igfs_file.h", "messages.h", "utils.h"],
    deps = [
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal"
    ],
    alwayslink = 1,
)

# TODO: Rewrite.
# This test is set to manual because it requires downloading the Ignite
# distribution to run. To run this test:
tf_cc_test(
    name = "ignite_file_system_test",
    size = "small",
    srcs = [
        "ignite_file_system_test.cc",
    ],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        ":ignite_file_system",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)
