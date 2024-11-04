set(SOURCE_FILES
    configs.cpp
    configs.hpp
    enums.cpp
    enums.hpp
    utils.cpp
    utils.hpp
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(Cryptopp REQUIRED IMPORTED_TARGET libcrypto++)

# Define a library target for the Common module
add_library(COMMON ${SOURCE_FILES})

# Include directories for header files
target_include_directories(COMMON PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(COMMON PUBLIC PkgConfig::Cryptopp)
