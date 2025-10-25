cmake_minimum_required(VERSION 4.0)
project(SQLiteCpp VERSION 1.0)

add_library(SQLiteCpp STATIC
    src/Backup.cpp
    src/Column.cpp
    src/Database.cpp
    src/Exception.cpp
    src/Savepoint.cpp
    src/Statement.cpp
    src/Transaction.cpp
)

target_include_directories(SQLiteCpp PUBLIC include ../sqlite3)