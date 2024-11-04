find_package(Boost 1.80.0 REQUIRED COMPONENTS program_options)

set(SOURCE_FILES
    parsing_trace.cpp
)
add_executable(parsing_trace ${SOURCE_FILES})
target_include_directories(parsing_trace PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(parsing_trace PUBLIC ${Boost_LIBRARIES})


set(SOURCE_FILES
    workload_prep.cpp
)
add_executable(workload_prep ${SOURCE_FILES})
target_include_directories(workload_prep PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(workload_prep PUBLIC CloudAPIs COMMON)
target_link_libraries(workload_prep PUBLIC ${Boost_LIBRARIES})


set(SOURCE_FILES
    workload_runner.cpp
    workload_runner.hpp
)
add_executable(workload_runner ${SOURCE_FILES})
target_include_directories(workload_runner PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(workload_runner PUBLIC MacaronClient COMMON CacheEngineLibrary)
target_link_libraries(workload_runner PUBLIC ${Boost_LIBRARIES})

