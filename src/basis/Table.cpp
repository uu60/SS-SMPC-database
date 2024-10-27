//
// Created by 杜建璋 on 2024/10/25.
//

#include <utility>

#include "basis/Table.h"
#include <iostream>
#include <iomanip>

Table::Table(std::string tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes) {
    this->tableName = std::move(tableName);
    this->fieldNames = std::move(fieldNames);
    this->fieldTypes = std::move(fieldTypes);
}

bool Table::insertRecord(const Record& r) {
    std::string command = "insert";
    Comm::send(&command, 0);
    Comm::send(&command, 0);
    this->records.push_back(r);
    return true;
}

const std::vector<int> &Table::getFieldTypes() {
    return fieldTypes;
}

const std::vector<Record>& Table::getAllRecords() const {
    return this->records;
}

const std::vector<std::string> &Table::getFieldNames() const {
    return fieldNames;
}


