//
// Created by 杜建璋 on 2024/11/1.
//

#include <utility>
#include <cmath>
#include <mpc_package/utils/Log.h>

#include "../../include/basis/TempRecord.h"

int TempRecord::getType(int valueIdx) const {
    return _types[valueIdx];
}

void TempRecord::addType(int type) {
    _types.push_back(type);
}

int TempRecord::getIdx(const std::string &fieldName) const {
    for (int i = 0; i < _fieldNames.size(); i++) {
        if (_fieldNames[i] == fieldName) {
            return i;
        }
    }
    return -1;
}

template<typename T>
BitSecret compareFieldsT(
    const std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<
        int64_t> > > &fieldValues,
    const std::vector<std::variant<BitSecret, IntSecret<int8_t>, IntSecret<int16_t>, IntSecret<int32_t>, IntSecret<
        int64_t> > > &otherFieldValues,
    int idx) {
    auto s0 = std::get<IntSecret<T> >(fieldValues[idx]);
    auto s1 = std::get<IntSecret<T> >(otherFieldValues[idx]);
    return s0.compare(s1);
}

BitSecret TempRecord::compareField(const TempRecord &other, const std::string &fieldName) const {
    int idx = getIdx(fieldName);
    int type = getType(idx);

    switch (type) {
        case 8:
            return compareFieldsT<int8_t>(_fieldValues, other._fieldValues, idx);
        case 16:
            return compareFieldsT<int16_t>(_fieldValues, other._fieldValues, idx);
        case 32:
            return compareFieldsT<int32_t>(_fieldValues, other._fieldValues, idx);
        default:
            return compareFieldsT<int64_t>(_fieldValues, other._fieldValues, idx);
    }
}
