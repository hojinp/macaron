set(SOURCE_FILES
    meringue.cpp
    meringue.hpp
    cache_item.cpp
    cache_item.hpp
)

find_package(Boost 1.80.0 REQUIRED COMPONENTS thread)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Create a library from the source files
add_executable(meringue ${SOURCE_FILES})

# Include directories
target_include_directories(meringue PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(meringue PUBLIC $ENV{HOME}/.local/include)
target_include_directories(meringue PUBLIC ${Boost_INCLUDE_DIRS})

target_link_libraries(meringue PRIVATE gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(meringue PRIVATE CloudAPIs ProtoLibrary COMMON CacheLibrary CacheEngineLibrary)
target_link_libraries(meringue PRIVATE /usr/local/lib/librocksdb.so)
target_link_libraries(meringue PRIVATE ${Boost_LIBRARIES})

