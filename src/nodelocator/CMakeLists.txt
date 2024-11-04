set(SOURCE_FILES
    consistent_hashing.cpp
    consistent_hashing.hpp
)

add_library(NodeLocatorLib ${SOURCE_FILES})

target_include_directories(NodeLocatorLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(NodeLocatorLib PUBLIC COMMON)
