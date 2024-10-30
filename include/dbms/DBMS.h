//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_DBMS_H
#define SMPC_DATABASE_DBMS_H

#include <iostream>
#include <vector>
#include <map>
#include "../basis/Database.h"

class DBMS {
public:
    static const std::string CDB_PRE;
    static const std::string DDB_PRE;
    static const std::string USE_PRE;
    static const std::string CTB_PRE;
    static const std::string DTB_PRE;
    static const std::string INS_PRE;
    static const std::string SEL_PRE;
private:
    std::map<std::string, Database> databases;
    Database *currentDatabase = nullptr;

    // private constructor
    DBMS() = default;

    // forbid clone
    DBMS(const DBMS &) = delete;

    DBMS &operator=(const DBMS &) = delete;

public:
    static DBMS &getInstance();

    void handleFileCommands();

    void handleConsole();

    void handleRequests();

    bool createDatabase(const std::string &dbName, std::string& msg);

    bool deleteDatabase(const std::string &dbName, std::string& msg);

    bool useDatabase(const std::string &dbName, std::string& msg);

    void executeSQL(const std::string &command);

private:
    void handleFailedExecution();

    void doCreateTable(std::istringstream &iss);

    static int parseDataType(const std::string &typeName);

    static void log(const std::string &msg, bool success);

};

#endif //SMPC_DATABASE_DBMS_H
