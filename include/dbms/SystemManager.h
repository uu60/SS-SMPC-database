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
#include <sql/SQLStatement.h>

using json = nlohmann::json;

class SystemManager {
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
    SystemManager() = default;

public:
    // forbid clone
    SystemManager(const SystemManager &) = delete;

    SystemManager &operator=(const SystemManager &) = delete;

    static SystemManager &getInstance();

    bool createDatabase(const std::string &dbName, std::string &msg);

    bool dropDatabase(const std::string &dbName, std::string &msg);

    bool useDatabase(const std::string &dbName, std::string &msg);

    static void log(const std::string &msg, bool success);

    void serverSelect(nlohmann::basic_json<> j) const;

    void serverInsert(nlohmann::basic_json<> j);

    Database *currentDatabase();

    void handleFailedExecution();

    static int parseDataType(const std::string &typeName);

    void execute(const std::string &command);

    static void notifyServersSync(json &j);

    void handleRequests();

private:
    // for client
    bool clientCreateTable(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    bool clientDropTable(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    bool clientCreateDeleteDb(std::istringstream &iss, std::ostringstream &resp, std::string &word, bool create);

    void clientUseDb(std::istringstream &iss, std::ostringstream &resp);

    bool clientInsert(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    bool clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt);
};

#endif //SMPC_DATABASE_DBMS_H
