//
// Created by 杜建璋 on 2024/10/25.
//

#include <utility>

#include "basis/Database.h"

Database::Database(std::string databaseName) {
    this->databaseName = std::move(databaseName);
}

std::string Database::getName() {
    return this->databaseName;
}

bool
Database::createTable(const std::string &tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes,
                      std::string &msg) {
    if (getTable(tableName)) {
        msg = "Table existed.";
        return false;
    }
    tables[tableName] = Table(tableName, std::move(fieldNames), std::move(fieldTypes));
    return true;
}

bool Database::deleteTable(const std::string &tableName, std::string &msg) {
    if (!getTable(tableName)) {
        msg = "Table not existed.";
        return false;
    }
    return true;
}

Table *Database::getTable(const std::string &tableName) {
    if (tables.find(tableName) == tables.end()) {
        return nullptr;
    }
    return &tables[tableName];
}

Database::Database() = default;
