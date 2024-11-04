set(SOURCE_FILES
    cost_lru_cache.cpp
    cost_lru_cache.hpp
    cost_lru_cache_lambda.cpp
    cost_lru_cache_lambda.hpp
    latency_lru_cache.cpp
    latency_lru_cache.hpp
    latency_lru_cache_lambda.cpp
    latency_lru_cache_lambda.cpp
    meringue_cache.cpp
    meringue_cache.hpp
)

# Create a library from the source files
add_library(CacheLibrary ${SOURCE_FILES})

# Include directories
target_include_directories(CacheLibrary PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(CacheLibrary PUBLIC CloudAPIs)

