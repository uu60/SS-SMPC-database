//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_TABLE_H
#define SMPC_DATABASE_TABLE_H
#include "./Record.h"
#include <vector>

class Table {
private:
    std::string _tableName;
    std::vector<std::string> _fieldNames;
    std::vector<int> _fieldTypes;
    std::vector<Record> _records;

public:
    Table() = default;

    explicit Table(std::string tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes);

    bool insert(const Record& r);

    [[nodiscard]] const std::vector<Record>& allRecords() const;

    const std::vector<std::string>& fieldNames() const;

    const std::vector<int>& fieldTypes();
};


#endif //SMPC_DATABASE_TABLE_H
