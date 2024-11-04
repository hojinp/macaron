set(SOURCE_FILES
    latency_generator.cpp
    latency_generator.hpp
)

add_library(LatencyLib ${SOURCE_FILES})

target_include_directories(LatencyLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(LatencyLib PUBLIC COMMON)
