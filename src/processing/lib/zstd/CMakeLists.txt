cmake_minimum_required(VERSION 4.0)
project(zstd VERSION 1.0)

# god bless https://stackoverflow.com/questions/63912743/compile-app-with-linked-dll-with-cmake-need-to-see-a-working-sample
add_library(zstd SHARED IMPORTED)
set_target_properties(zstd PROPERTIES LINKER_LANGUAGE C)
set_target_properties(zstd PROPERTIES IMPORTED_LOCATION libzstd.dll)
set_target_properties(zstd PROPERTIES IMPORTED_IMPLIB libzstd.dll.a)