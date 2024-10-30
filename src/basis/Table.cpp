//
// Created by 杜建璋 on 2024/10/25.
//

#include <utility>

#include "basis/Table.h"
#include <iostream>
#include <iomanip>

Table::Table(std::string tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes) {
    this->_tableName = std::move(tableName);
    this->_fieldNames = std::move(fieldNames);
    this->_fieldTypes = std::move(fieldTypes);
}

bool Table::insert(const Record& r) {
    std::string command = "insert";
    Comm::send(&command, 0);
    Comm::send(&command, 0);
    this->_records.push_back(r);
    return true;
}

const std::vector<int> &Table::fieldTypes() {
    return _fieldTypes;
}

const std::vector<Record>& Table::allRecords() const {
    return this->_records;
}

const std::vector<std::string> &Table::fieldNames() const {
    return _fieldNames;
}


