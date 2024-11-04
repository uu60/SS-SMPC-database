//
// Created by 杜建璋 on 2024/11/1.
//

#ifndef TEMPRECORD_H
#define TEMPRECORD_H
#include "./AbstractRecord.h"

class TempRecord : public AbstractRecord {
public:
    std::vector<std::string> _fieldNames;
    std::vector<int32_t> _types;
    BitSecret _valid = BitSecret(Comm::rank());
    bool _padding{};

    [[nodiscard]] int getType(int valueIdx) const override;

    void addType(int type) override;

    [[nodiscard]] int getIdx(const std::string &fieldName) const override;

    [[nodiscard]] BitSecret compareField(const TempRecord &other, const std::string &fieldName) const;
};


#endif //TEMPRECORD_H
