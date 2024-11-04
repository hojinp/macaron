set(SOURCE_FILES
    macaron_client.cpp
    macaron_client.hpp
)

find_package(Boost 1.80.0 REQUIRED COMPONENTS thread)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Create a library from the source files
add_library(MacaronClient ${SOURCE_FILES})


# Include directories
target_include_directories(MacaronClient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(MacaronClient PUBLIC $ENV{HOME}/.local/include)
target_include_directories(MacaronClient PUBLIC ${Boost_INCLUDE_DIRS})

target_link_libraries(MacaronClient PRIVATE gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(MacaronClient PUBLIC NodeLocatorLib CloudAPIs ProtoLibrary COMMON )
target_link_libraries(MacaronClient PRIVATE ${Boost_LIBRARIES})
