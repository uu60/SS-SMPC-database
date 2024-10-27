cmake_minimum_required(VERSION 3.22)
project(SMPC_database)

set(CMAKE_CXX_STANDARD 20)

find_package(mpc_package REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(SMPC_database src/main.cpp
        src/basis/Table.cpp
        include/basis/Table.h
        src/dbms/DBMS.cpp
        include/dbms/DBMS.h
        src/basis/Record.cpp
        include/basis/Record.h
        src/basis/Database.cpp
        include/basis/Database.h)
target_link_libraries(${PROJECT_NAME} mpc_package)
target_link_directories(${PROJECT_NAME} PUBLIC ${mpc_package_LIBRARY_DIRS})
target_include_directories(${PROJECT_NAME} PUBLIC ${mpc_package_INCLUDE_DIRS})
