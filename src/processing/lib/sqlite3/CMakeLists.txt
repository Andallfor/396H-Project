cmake_minimum_required(VERSION 4.0)
project(sqlite3 VERSION 1.0)

add_library(sqlite3 STATIC
    sqlite3.c
)
target_include_directories(sqlite3 PUBLIC .)