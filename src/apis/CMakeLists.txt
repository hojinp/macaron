set(SOURCE_FILES
    cloudapis.cpp
    cloudapis.hpp
    redisapis.cpp
    redisapis.hpp
    rocksdbapis.cpp
    rocksdbapis.hpp
)

# Create a library from the source files
add_library(CloudAPIs ${SOURCE_FILES})

# Include packages
find_package(AWSSDK REQUIRED COMPONENTS s3 lambda ec2)
find_path(HIREDIS_HEADER hiredis)
find_library(HIREDIS_LIB hiredis)
find_path(REDIS_PLUS_PLUS_HEADER sw)
find_library(REDIS_PLUS_PLUS_LIB redis++)


# Link AWS SDK
target_link_libraries(CloudAPIs PRIVATE ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(CloudAPIs PRIVATE ${HIREDIS_LIB})
target_link_libraries(CloudAPIs PRIVATE ${REDIS_PLUS_PLUS_LIB})
target_link_libraries(CloudAPIs PRIVATE /usr/local/lib/librocksdb.so)
target_link_libraries(CloudAPIs PUBLIC COMMON)

# Include directories
target_include_directories(CloudAPIs PRIVATE ${AWSSDK_INCLUDE_DIRS})
target_include_directories(CloudAPIs PRIVATE ${HIREDIS_HEADER})
target_include_directories(CloudAPIs PRIVATE ${REDIS_PLUS_PLUS_HEADER})
target_include_directories(CloudAPIs PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

