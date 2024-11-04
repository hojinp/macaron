set(GRPC_SRCS
    cache_engine.grpc.pb.cc
    mbakery.grpc.pb.cc
    meringue.grpc.pb.cc
)

set(GRPC_HDRS
    cache_engine.grpc.pb.h
    mbakery.grpc.pb.h
    meringue.grpc.pb.h
)

set(PROTO_SRCS
    cache_engine.pb.cc
    mbakery.pb.cc
    meringue.pb.cc
)

set(PROTO_HDRS
    cache_engine.pb.h
    mbakery.pb.h
    meringue.pb.h
)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

add_library(ProtoLibrary ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})

if (Protobuf_FOUND)
    message(STATUS "Found Protobuf version: ${Protobuf_VERSION}")
else ()
    message(FATAL_ERROR "Protobuf not found")
endif ()

target_link_libraries(ProtoLibrary PRIVATE gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(ProtoLibrary PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(ProtoLibrary PUBLIC $ENV{HOME}/.local/include)
