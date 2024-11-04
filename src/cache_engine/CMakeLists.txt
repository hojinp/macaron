set(SOURCE_FILES
    cache_engine.cpp
    cache_engine.hpp
    cache_engine_logger.cpp
    cache_engine_logger.hpp
    packing_block_manager.cpp
    packing_block_manager.hpp
)

find_package(Boost 1.80.0 REQUIRED COMPONENTS thread)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(Cryptopp REQUIRED IMPORTED_TARGET libcrypto++)

# Create a library from the source files
add_executable(cache_engine ${SOURCE_FILES})

# Include directories
target_include_directories(cache_engine PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(cache_engine PUBLIC $ENV{HOME}/.local/include)
target_include_directories(cache_engine PUBLIC ${Boost_INCLUDE_DIRS})

# Link libraries
target_link_libraries(cache_engine PRIVATE gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(cache_engine PUBLIC COMMON CloudAPIs ProtoLibrary NodeLocatorLib)
target_link_libraries(cache_engine PUBLIC PkgConfig::Cryptopp)
target_link_libraries(cache_engine PRIVATE ${Boost_LIBRARIES})



set(SOURCE_FILES
    cache_engine_logger.cpp
    cache_engine_logger.hpp
)

add_library(CacheEngineLibrary ${SOURCE_FILES})

target_include_directories(CacheEngineLibrary PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(CacheEngineLibrary PUBLIC COMMON CloudAPIs ProtoLibrary NodeLocatorLib)
target_link_libraries(CacheEngineLibrary PUBLIC PkgConfig::Cryptopp)
