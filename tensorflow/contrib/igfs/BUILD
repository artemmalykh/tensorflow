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
    name = "igfs",
    srcs = ["__init__.py"],
    srcs_version = "PY2AND3",
    deps = [
        ":igfs_ops",
    ],
)

tf_custom_op_library(
    name = "_igfs_ops.so",
    srcs = [
        "ops/igfs_ops.cc",
        "kernels/ignite_file_system.h",
    ],
    deps = [":igfs_kernels"],
)

tf_gen_op_libs(
    op_lib_names = ["igfs_ops"],
    deps = [":igfs_kernels"]
)

cc_library(
    name = "igfs_kernels",
    hdrs = [
        "kernels/igfs_protocol.h",
        "kernels/ignite_file_system.h",
        "kernels/messages.h",
        "kernels/utils.h",
    ],
    srcs = [
        "kernels/igfs_protocol.cc",
        "kernels/ignite_file_system.cc",
        "kernels/messages.cc",
    ],
    deps = [
        "//tensorflow/contrib/ignite:dataset_kernels",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal"
    ],
    alwayslink=1,
)

py_library(
    name = "igfs_ops",
    srcs = [
        "python/ops/igfs_ops.py",
    ],
    srcs_version = "PY2AND3",
    deps = [
        ":igfs_op_loader",
        "//tensorflow/python:dataset_ops_gen",
        "//tensorflow/python:util",
        "//tensorflow/python/data/ops:dataset_ops",
        "//tensorflow/python/data/util:nest",
    ],
)

tf_gen_op_wrapper_py(
    name = "gen_igfs_ops",
    out = "python/ops/gen_igfs_ops.py",
    deps = ["//tensorflow/contrib/igfs:igfs_ops_op_lib"],
)

tf_kernel_library(
    name = "igfs_ops_kernels",
    deps = [
        ":igfs_kernels",
        "//tensorflow/core:framework",
    ],
    alwayslink = 1,
)

tf_custom_op_py_library(
    name = "igfs_op_loader",
    srcs = ["python/ops/igfs_op_loader.py"],
    dso = ["//tensorflow/contrib/igfs:_igfs_ops.so"],
    kernels = [
        ":igfs_ops_kernels",
        "//tensorflow/contrib/ignite:igfs_ops_op_lib",
    ],
    srcs_version = "PY2AND3",
    deps = [
        ":gen_igfs_ops",
        "//tensorflow/contrib/util:util_py",
        "//tensorflow/python:platform",
    ],
)
