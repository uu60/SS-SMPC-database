//
// Created by 杜建璋 on 2024/10/25.
//

#ifndef SMPC_DATABASE_RECORD_H
#define SMPC_DATABASE_RECORD_H

#include <vector>
#include <variant>
#include <mpc_package/api/IntSecret.h>
#include <mpc_package/api/BitSecret.h>

class Table;

class Record {
private:
    Table *_owner;
    std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>>> _fieldValues;

public:
    explicit Record(Table *owner);

    void addField(std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>> secret, int type);

    void print() const;

    [[nodiscard]] const std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>>>& fieldValues() const;

    [[nodiscard]] Table *owner() const;
};


#endif //SMPC_DATABASE_RECORD_H
