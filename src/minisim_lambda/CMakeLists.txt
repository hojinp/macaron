find_package(jsoncpp REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(Cryptopp REQUIRED IMPORTED_TARGET libcrypto++)

find_package(aws-lambda-runtime REQUIRED)

set(SOURCE_FILES
    latency_minisim_lambda.cpp
    latency_minisim_lambda.hpp
)

add_executable(latency_minisim_lambda ${SOURCE_FILES})
target_link_libraries(latency_minisim_lambda PRIVATE AWS::aws-lambda-runtime)
target_link_libraries(latency_minisim_lambda PRIVATE jsoncpp_lib_static)
target_link_libraries(latency_minisim_lambda PUBLIC PkgConfig::Cryptopp)
target_include_directories(latency_minisim_lambda PRIVATE ${JSONCPP_SOURCE_DIR}/include)
target_link_libraries(latency_minisim_lambda PUBLIC COMMON LatencyLib CloudAPIs CacheLibrary CacheEngineLibrary)
aws_lambda_package_target(latency_minisim_lambda)

set(SOURCE_FILES
    cost_minisim_lambda.cpp
    cost_minisim_lambda.hpp
)

add_executable(cost_minisim_lambda ${SOURCE_FILES})
target_link_libraries(cost_minisim_lambda PRIVATE AWS::aws-lambda-runtime)
target_link_libraries(cost_minisim_lambda PRIVATE jsoncpp_lib_static)
target_link_libraries(cost_minisim_lambda PUBLIC PkgConfig::Cryptopp)
target_include_directories(cost_minisim_lambda PRIVATE ${JSONCPP_SOURCE_DIR}/include)
target_link_libraries(cost_minisim_lambda PUBLIC COMMON CloudAPIs CacheLibrary CacheEngineLibrary)
aws_lambda_package_target(cost_minisim_lambda)
