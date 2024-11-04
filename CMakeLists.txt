cmake_minimum_required(VERSION 3.12)
project(Macaron)

# Set C++ standard (e.g., C++17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Include subdirectories
add_subdirectory(src)

set(InstallTargets

    parsing_trace
    workload_prep
    workload_runner

    cost_minisim_lambda
    latency_minisim_lambda
    mbakery
    meringue
    cache_engine
)
install(TARGETS ${InstallTargets} DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
file(COPY ${CMAKE_SOURCE_DIR}/src/conf/redis.conf DESTINATION ${CMAKE_INSTALL_PREFIX}/conf)
file(COPY ${CMAKE_SOURCE_DIR}/data/latency DESTINATION ${CMAKE_INSTALL_PREFIX}/data)
