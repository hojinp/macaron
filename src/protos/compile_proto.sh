#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
"$HOME"/tools/grpc/cmake/build/third_party/protobuf/protoc --proto_path=${SCRIPT_DIR} --cpp_out=${SCRIPT_DIR} --grpc_out=${SCRIPT_DIR} --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` *.proto
