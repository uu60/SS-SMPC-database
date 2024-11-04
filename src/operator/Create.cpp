//
// Created by 杜建璋 on 2024/11/3.
//

#include "../../include/operator/Create.h"

#include <sstream>
#include <hsql/SQLParser.h>

#include "../../include/dbms/SystemManager.h"

bool Create::clientCreateTable(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    const auto *createStmt = dynamic_cast<const hsql::CreateStatement *>(stmt);
    if (createStmt->type != hsql::kCreateTable) {
        resp << "Unsupported CREATE statement type." << std::endl;
        return false;
    }

    std::string tableName = createStmt->tableName;

    if (SystemManager::getInstance()._currentDatabase->getTable(tableName)) {
        resp << "Failed. Table `" + tableName + "` already exists." << std::endl;
        return false;
    }

    std::vector<std::string> fieldNames;
    std::vector<int> fieldTypes;

    for (const auto *column: *createStmt->columns) {
        std::string fieldName = column->name;

        int type;
        switch (column->type.data_type) {
            case hsql::DataType::BOOLEAN:
                type = 1;
            break;
            case hsql::DataType::INT:
                type = static_cast<int>(column->type.length);
            if (type != 8 && type != 16 && type != 32 && type != 64) {
                goto err;
            }
            break;
            default:
                err:
                    resp << "Failed. Unsupported data type for field `" + fieldName + "`." << std::endl;
            return false;
        }

        fieldNames.push_back(fieldName);
        fieldTypes.push_back(type);
    }

    std::string msg;
    if (!SystemManager::getInstance()._currentDatabase->createTable(tableName, fieldNames, fieldTypes, msg)) {
        resp << "Failed. " << msg << std::endl;
        return false;
    }

    // notify servers
    json j;
    j["type"] = SystemManager::getCommandPrefix(SystemManager::CREATE_TABLE);
    j["name"] = tableName;
    j["fieldNames"] = fieldNames;
    j["fieldTypes"] = fieldTypes;
    SystemManager::notifyServersSync(j);

    resp << "OK. Table `" + tableName + "` created." << std::endl;
    return true;
}

void Create::serverCreateTable(json &j) {
    std::string tbName = j.at("name").get<std::string>();
    std::vector<std::string> fieldNames = j.at("fieldNames").get<std::vector<std::string> >();
    std::vector<int32_t> fieldTypes = j.at("fieldTypes").get<std::vector<int32_t> >();
    std::string msg;
    SystemManager::getInstance()._currentDatabase->createTable(tbName, fieldNames, fieldTypes, msg);
}