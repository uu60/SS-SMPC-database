cmake_minimum_required(VERSION 3.22)
project(SMPC_database)

set(CMAKE_CXX_STANDARD 20)
set(SQLPARSER_LIB /usr/local/lib/libsqlparser.so)

find_package(mpc_package REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/include /usr/local/include/hsql /usr/local/include/tabulate/include /usr/local/include/json/include)

add_executable(SMPC_database src/main.cpp
        src/basis/Table.cpp
        include/basis/Table.h
        src/dbms/SystemManager.cpp
        include/dbms/SystemManager.h
        src/basis/AbstractRecord.cpp
        include/basis/AbstractRecord.h
        src/basis/Database.cpp
        include/basis/Database.h
        src/socket/LocalServer.cpp
        include/socket/LocalServer.h
        src/basis/TableRecord.cpp
        include/basis/TableRecord.h
        src/basis/TempRecord.cpp
        include/basis/TempRecord.h
        include/operator/Select.h
        src/operator/Select.cpp
        include/operator/Insert.h
        src/operator/Insert.cpp
        include/operator/Create.h
        src/operator/Create.cpp
        include/operator/Drop.h
        src/operator/Drop.cpp
        src/function/Order.cpp
        include/function/Order.h
)

target_link_libraries(${PROJECT_NAME} mpc_package ${SQLPARSER_LIB})
target_link_directories(${PROJECT_NAME} PUBLIC ${mpc_package_LIBRARY_DIRS})
target_include_directories(${PROJECT_NAME} PUBLIC ${mpc_package_INCLUDE_DIRS})
