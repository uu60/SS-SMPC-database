//
// Created by 杜建璋 on 2024/10/25.
//

#include "basis/Record.h"
#include <iostream>
#include <iomanip>
#include "basis/Table.h"

Record::Record(Table *owner) {
    this->_owner = owner;
}

void Record::print(std::ostringstream& oss) const {
    for (int i = 0; i < _fieldValues.size(); i++) {
        int type = _types[i];
        if (type == 1) {
            oss << std::setw(10) << (int64_t) std::get<BitSecret>(_fieldValues[i]).get();
        } else if (type == 8) {
            oss << std::setw(10) << (int64_t) std::get<IntSecret<int8_t>>(_fieldValues[i]).get();
        } else if (type == 16) {
            oss << std::setw(10) << (int64_t) std::get<IntSecret<int16_t>>(_fieldValues[i]).get();
        } else if (type == 32) {
            oss << std::setw(10) << (int64_t) std::get<IntSecret<int32_t>>(_fieldValues[i]).get();
        } else {
            oss << std::setw(10) << (int64_t) std::get<IntSecret<int64_t>>(_fieldValues[i]).get();
        }
    }
    oss << std::endl;
}

void Record::addField(std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>> secret, int type) {
    _types.push_back(type);
    if (type == 1) {
        this->_fieldValues.emplace_back(std::get<BitSecret>(secret));
    } else if (type == 8) {
        this->_fieldValues.emplace_back(std::get<IntSecret<int8_t>>(secret));
    } else if (type == 16) {
        this->_fieldValues.emplace_back(std::get<IntSecret<int16_t>>(secret));
    } else if (type == 32) {
        this->_fieldValues.emplace_back(std::get<IntSecret<int32_t>>(secret));
    } else {
        this->_fieldValues.emplace_back(std::get<IntSecret<int64_t>>(secret));
    }
}

Table *Record::owner() const {
    return _owner;
}

const std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<int64_t>>> &
Record::fieldValues() const {
    return this->_fieldValues;
}
