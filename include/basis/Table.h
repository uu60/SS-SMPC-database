//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_TABLE_H
#define SMPC_DATABASE_TABLE_H
#include "./Record.h"
#include <vector>

class Table {
private:
    std::string tableName;
    std::vector<std::string> fieldNames;
    std::vector<int> fieldTypes;
    std::vector<Record> records;

public:
    Table() = default;

    explicit Table(std::string tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes);

    bool insertRecord(const Record& r);

    const std::vector<Record>& getAllRecords() const;

    const std::vector<std::string>& getFieldNames() const;

    const std::vector<int>& getFieldTypes();
};


#endif //SMPC_DATABASE_TABLE_H
