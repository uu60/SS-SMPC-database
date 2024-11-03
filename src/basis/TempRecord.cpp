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

TempRecord TempRecord::getSortPaddingRecord() {
    TempRecord t;
    t._types.push_back(-1);
    return t;
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

template<typename T>
void muxSwapHelper(TempRecord &first, TempRecord &second, int fieldIndex, BitSecret swap) {
    auto s0 = std::get<IntSecret<T> >(first._fieldValues[fieldIndex]);
    auto s1 = std::get<IntSecret<T> >(second._fieldValues[fieldIndex]);
    first._fieldValues[fieldIndex] = IntSecret<T>::mux(s1, s0, swap);
    second._fieldValues[fieldIndex] = IntSecret<T>::mux(s0, s1, swap);
}

void TempRecord::muxSwap(TempRecord &first, TempRecord &second, BitSecret swap) {
    // Swap each field in 2 records
    for (int i = 0; i < first._fieldValues.size(); i++) {
        int type = first.getType(i);
        // Select type and call template function
        switch (type) {
            case 8:
                muxSwapHelper<int8_t>(first, second, i, swap);
                break;
            case 16:
                muxSwapHelper<int16_t>(first, second, i, swap);
                break;
            case 32:
                muxSwapHelper<int32_t>(first, second, i, swap);
                break;
            default:
                muxSwapHelper<int64_t>(first, second, i, swap);
                break;
        }
    }
    auto v1 = first._valid;
    auto v2 = second._valid;
    first._valid = BitSecret::mux(v2, v1, swap);
    second._valid = BitSecret::mux(v1, v2, swap);
}

// Compare two records based on multiple columns and their sort orders
BitSecret TempRecord::requiresSwap(const TempRecord &r0, const TempRecord &r1,
                                   const std::vector<std::string> &orderFields,
                                   const std::vector<BitSecret> &ascendingOrders) {
    // padding record
    std::vector<BitSecret> obeys;
    std::vector<BitSecret> eqs;
    size_t size = orderFields.size();
    for (size_t i = 0; i < size; ++i) {
        const auto &fieldName = orderFields[i];
        BitSecret ascending = ascendingOrders[i];

        // Secret comparison on the column values
        // c0 = r1 < r2
        BitSecret lt = r0.compareField(r1, fieldName);
        // c1 = r1 > r2
        BitSecret gt = r1.compareField(r0, fieldName);
        BitSecret eq = lt.not_().and_(gt.not_());

        obeys.push_back(ascending.and_(gt).or_(ascending.not_().and_(lt)));
        eqs.push_back(eq);
    }

    // obey0 || (eq0 & (obey1 || (eq1 ... & obey_n)))
    BitSecret ret = obeys[size - 1];
    for (size_t i = size - 2; i < size; --i) {
        ret = ret.and_(eqs[i]).or_(obeys[i]);
    }
    return ret;
}

void TempRecord::bitonicSort(std::vector<TempRecord> &records, const std::vector<std::string> &fieldNames,
                             const std::vector<BitSecret> &ascendingOrders) {
    size_t N = records.size();
    auto is_power_of_two = [](size_t n) {
        return n && (!(n & (n - 1)));
    };

    if (!is_power_of_two(N)) {
        TempRecord padding = records[0];
        padding._valid = BitSecret(false);
        size_t next_power = 1 << static_cast<size_t>(std::ceil(std::log2(N)));
        records.resize(next_power, padding);
        N = next_power;
    }

    // Start with sequences of size 2 and double the size each time
    for (size_t k = 2; k <= N; k <<= 1) {
        // Start merging sequences from half of k and halve the size each time
        for (size_t j = k >> 1; j > 0; j >>= 1) {
            for (size_t i = 0; i < N; i++) {
                size_t ixj = i ^ j;
                if (ixj > i) {
                    // Determine the direction of sorting
                    bool dir = (i & k) == 0;
                    BitSecret swap = requiresSwap(records[i], records[ixj], fieldNames, ascendingOrders);
                    // Invert swap condition if direction is descending
                    if (!dir) {
                        swap = swap.not_();
                    }
                    muxSwap(records[i], records[ixj], swap);
                }
            }
        }
    }
}
