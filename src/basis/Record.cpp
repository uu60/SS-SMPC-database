//
// Created by 杜建璋 on 2024/10/25.
//

#include "basis/Record.h"
#include <iostream>
#include <iomanip>
#include "basis/Table.h"

Record::Record(Table *owner) {
    this->owner = owner;
}

void Record::print() const {
    for (int i = 0; i < values.size(); i++) {
        int type = owner->getFieldTypes()[i];
        if (type == 1) {
            std::cout << std::setw(10) << (int64_t) std::get<BitSecret>(values[i]).get();
        } else if (type == 8) {
            std::cout << std::setw(10) << (int64_t) std::get<IntSecret<int8_t>>(values[i]).get();
        } else if (type == 16) {
            std::cout << std::setw(10) << (int64_t) std::get<IntSecret<int16_t>>(values[i]).get();
        } else if (type == 32) {
            std::cout << std::setw(10) << (int64_t) std::get<IntSecret<int32_t>>(values[i]).get();
        } else {
            std::cout << std::setw(10) << (int64_t) std::get<IntSecret<int64_t>>(values[i]).get();
        }
    }
    std::cout << std::endl;
}

void Record::addField(std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>> secret, int type) {
    if (type == 1) {
        this->values.emplace_back(std::get<BitSecret>(secret));
    } else if (type == 8) {
        this->values.emplace_back(std::get<IntSecret<int8_t>>(secret));
    } else if (type == 16) {
        this->values.emplace_back(std::get<IntSecret<int16_t>>(secret));
    } else if (type == 32) {
        this->values.emplace_back(std::get<IntSecret<int32_t>>(secret));
    } else {
        this->values.emplace_back(std::get<IntSecret<int64_t>>(secret));
    }
}

Table *Record::getOwner() const {
    return owner;
}

const std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>>> &
Record::getValues() const {
    return this->values;
}
