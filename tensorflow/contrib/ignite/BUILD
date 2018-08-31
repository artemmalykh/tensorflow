package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

load(
    "//tensorflow:tensorflow.bzl",
    "tf_gen_op_wrapper_py",
    "tf_kernel_library",
    "tf_custom_op_library",
    "tf_custom_op_py_library",
    "tf_gen_op_libs",
    "tf_py_test",
    "tf_cc_test",
    "tf_cc_shared_object",
    "if_not_windows",
    "if_windows",
    "tf_cc_binary"
)

py_library(
    name = "ignite",
    srcs = ["__init__.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":dataset_ops",
    ],
)

tf_custom_op_library(
    name = "_dataset_ops.so",
    srcs = ["ops/dataset_ops.cc"],
    deps = [":dataset_kernels"],
)

#tf_cc_shared_object(
#   name = "igfs/ignite_file_system.so",
#   srcs = ["igfs/ignite_file_system.cc"],
#   copts = if_not_windows(["-Wno-sign-compare"]),
#   linkopts = select({
#       "//conditions:default": [
#           "-lm",
#       ],
#       "//tensorflow:darwin": [],
#       "//tensorflow:windows": [],
#   }),
#   deps = [
#       "//tensorflow/core:framework_headers_lib",
#       "//third_party/eigen3",
#       "@protobuf_archive//:protobuf_headers",
#   ],
#)

tf_gen_op_libs(
    op_lib_names = ["dataset_ops"],
)

tf_cc_shared_object(
    name = "ignite_file_system.so",
    srcs = [
        "igfs/igfs_protocol.h",
        "igfs/igfs_protocol.cc",
        "igfs/ignite_file_system.h",
        "igfs/ignite_file_system.cc",
        "igfs/messages.h",
        "igfs/messages.cc",
        "igfs/utils.h",
        ],
    deps = [
        ":dataset_kernels",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal"
    ],
    #linkshared = 1,
    #alwayslink=1,
)

cc_library(
    name = "dataset_kernels",
    hdrs = [
        "kernels/ignite_client.h",
        "kernels/ignite_plain_client.h",
        "kernels/ignite_ssl_wrapper.h",
    ],
    srcs = [
        "kernels/ignite_dataset_ops.cc",
        "kernels/ignite_client.h",
        "kernels/ignite_plain_client.h",
        "kernels/ignite_ssl_wrapper.h",
        "kernels/ignite_ssl_wrapper.cc",
        "kernels/ignite_binary_object_parser.h",
        "kernels/ignite_binary_object_parser.cc",
        "kernels/ignite_dataset.h",
        "kernels/ignite_dataset.cc",
        "kernels/ignite_dataset_iterator.h",
        "kernels/ignite_dataset_iterator.cc",
    ] + if_not_windows([
        "kernels/ignite_plain_client_unix.cc",
    ]) + if_windows([
        "kernels/ignite_plain_client_windows.cc",
    ]),
    deps = [
        "//tensorflow/core:framework_headers_lib",
        "//third_party/eigen3",
        "@boringssl//:ssl",
        "@protobuf_archive//:protobuf_headers",
    ],
    alwayslink = 1,
)

py_library(
    name = "dataset_ops",
    srcs = [
        "python/ops/ignite_dataset_ops.py",
    ],
    srcs_version = "PY2AND3",
    deps = [
        ":ignite_op_loader",
        "//tensorflow/python:dataset_ops_gen",
        "//tensorflow/python:util",
        "//tensorflow/python/data/ops:dataset_ops",
        "//tensorflow/python/data/util:nest",
    ],
)

tf_gen_op_wrapper_py(
    name = "gen_dataset_ops",
    out = "python/ops/gen_dataset_ops.py",
    deps = ["//tensorflow/contrib/ignite:dataset_ops_op_lib"],
)

tf_kernel_library(
    name = "dataset_ops_kernels",
    deps = [
        ":dataset_kernels",
        "//tensorflow/core:framework",
    ],
    alwayslink = 1,
)

tf_custom_op_py_library(
    name = "ignite_op_loader",
    srcs = ["python/ops/ignite_op_loader.py"],
    dso = ["//tensorflow/contrib/ignite:_dataset_ops.so"],
    kernels = [
        ":dataset_ops_kernels",
        "//tensorflow/contrib/ignite:dataset_ops_op_lib",
    ],
    srcs_version = "PY2AND3",
    deps = [
        ":gen_dataset_ops",
        "//tensorflow/contrib/util:util_py",
        "//tensorflow/python:platform",
    ],
)

# The Apache Ignite servers have to setup before the test and tear down
# after the test manually. The docker engine has to be installed.
#
# To setup Apache Ignite servers:
# $ bash ./python/tests/start_ignite.sh
#
# To tear down Apache Ignite servers:
# $ bash ./python/tests/stop_ignite.sh
tf_py_test(
    name = "ignite_dataset_test",
    srcs = ["python/tests/ignite_dataset_test.py"],
    additional_deps = [
        ":ignite",
        "//tensorflow/python:client_testlib",
        "//tensorflow/python:framework",
        "//tensorflow/python:framework_test_lib",
        "//tensorflow/python:platform_test",
    ],
    tags = [
        "manual",
        "no_windows",
        "notap",
    ],
)

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
