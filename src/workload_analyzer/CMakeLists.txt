find_package(Boost 1.80.0 REQUIRED COMPONENTS program_options)

set(SOURCE_FILES
    workload_analyzer.cpp
)
add_executable(workload_analyzer ${SOURCE_FILES})
target_include_directories(workload_analyzer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(workload_analyzer PUBLIC ${Boost_LIBRARIES})

