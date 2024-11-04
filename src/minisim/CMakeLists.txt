set(SOURCE_FILES
    cost_minisim.cpp
    cost_minisim.hpp
    latency_minisim.cpp
    latency_minisim.hpp
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(Cryptopp REQUIRED IMPORTED_TARGET libcrypto++) 

# Create a library from the source files
add_library(MiniSimLibrary ${SOURCE_FILES})

# Include directories
target_include_directories(MiniSimLibrary PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(MiniSimLibrary PUBLIC LatencyLib CacheLibrary CacheEngineLibrary)
target_link_libraries(MiniSimLibrary PUBLIC PkgConfig::Cryptopp)

