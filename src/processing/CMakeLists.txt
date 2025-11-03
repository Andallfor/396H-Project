cmake_minimum_required(VERSION 4.0)
project(main VERSION 1.0)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "-pedantic -Wall -Wextra -Wno-unused-variable -Wno-unused-parameter")

if (UNIX)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "out")
else()
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../../)
endif()

add_subdirectory(lib/sqlite3)

add_executable(main src/main.cpp)

add_library(zstd SHARED IMPORTED)
set_target_properties(zstd PROPERTIES LINKER_LANGUAGE C)
if (UNIX)
    set_target_properties(zstd PROPERTIES IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/lib/zstd/unix/libzstd.so")
    target_include_directories(main PUBLIC "${CMAKE_SOURCE_DIR}/lib/zstd/include")
else()
    set_target_properties(zstd PROPERTIES IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/lib/zstd/win/libzstd.dll")
    set_target_properties(zstd PROPERTIES IMPORTED_IMPLIB "${CMAKE_SOURCE_DIR}/lib/zstd/win/libzstd.dll.a")
endif()

target_include_directories(main PUBLIC lib/glaze lib/ctre include)
target_link_libraries(main PUBLIC sqlite3 zstd)