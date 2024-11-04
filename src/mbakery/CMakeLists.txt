set(SOURCE_FILES
    mbakery.cpp
    mbakery.hpp
)

find_package(Boost 1.80.0 REQUIRED COMPONENTS program_options)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Create a library from the source files
add_executable(mbakery ${SOURCE_FILES})


# Include directories
target_include_directories(mbakery PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mbakery PUBLIC $ENV{HOME}/.local/include)
target_include_directories(mbakery PUBLIC ${Boost_INCLUDE_DIRS})

target_link_libraries(mbakery PRIVATE gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(mbakery PUBLIC CloudAPIs ProtoLibrary COMMON CacheEngineLibrary MiniSimLibrary)
target_link_libraries(mbakery PUBLIC ${Boost_LIBRARIES})

