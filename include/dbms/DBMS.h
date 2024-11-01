//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_DBMS_H
#define SMPC_DATABASE_DBMS_H

#include <iostream>
#include <vector>
#include <map>
#include "../basis/Database.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class DBMS {
public:
    static const std::string INS_PRE;
    static const std::string SEL_PRE;
private:
    std::map<std::string, Database> _databases;
    Database *_currentDatabase = nullptr;

    // temp
    int done{};
    std::string msg{};

private:
    // private constructor
    DBMS() = default;

public:
    // forbid clone
    DBMS(const DBMS &) = delete;

    DBMS &operator=(const DBMS &) = delete;

    static DBMS &getInstance();

    bool createDatabase(const std::string &dbName, std::string &msg);

    bool dropDatabase(const std::string &dbName, std::string &msg);

    bool useDatabase(const std::string &dbName, std::string &msg);

    static void log(const std::string &msg, bool success);

    Database *currentDatabase();

    void handleFailedExecution();

    static int parseDataType(const std::string &typeName);

    void execute(const std::string &command);

    static void notifyServersSync(json& j);

    void handleRequests();
};

#endif //SMPC_DATABASE_DBMS_H
