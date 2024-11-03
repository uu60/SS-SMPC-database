//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_TABLE_H
#define SMPC_DATABASE_TABLE_H
#include <vector>

#include "./TableRecord.h"
#include "./TempRecord.h"

class Table {
private:
    std::string _tableName;
    std::vector<std::string> _fieldNames;
    std::vector<int> _fieldTypes;
    std::vector<TableRecord> _records;

public:
    Table() = default;

    explicit Table(std::string tableName, std::vector<std::string> fieldNames, std::vector<int> fieldTypes);

    bool insert(const TableRecord& r);

    [[nodiscard]] std::vector<TempRecord> selectAll() const;

    const std::vector<std::string>& fieldNames() const;

    const std::vector<int>& fieldTypes();

    void muxSwap(int i, int j, BitSecret c);
};


#endif //SMPC_DATABASE_TABLE_H
