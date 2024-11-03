//
// Created by 杜建璋 on 2024/10/25.
//

#include <utility>

#include "basis/Table.h"
#include <iostream>
#include <iomanip>

#include "basis/TableRecord.h"
#include "basis/TempRecord.h"

Table::Table(std::string tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes) {
    this->_tableName = std::move(tableName);
    this->_fieldNames = std::move(fieldNames);
    this->_fieldTypes = std::move(fieldTypes);
}

bool Table::insert(const TableRecord& r) {
    this->_records.push_back(r);
    return true;
}

const std::vector<int> &Table::fieldTypes() {
    return _fieldTypes;
}

std::vector<TempRecord> Table::selectAll() const {
    std::vector<TempRecord> ret;
    for (const auto& r : this->_records) {
        ret.push_back(r.convertToTemp());
    }
    return ret;
}

const std::vector<std::string> &Table::fieldNames() const {
    return _fieldNames;
}

