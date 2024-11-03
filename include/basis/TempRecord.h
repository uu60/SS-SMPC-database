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

protected:
    [[nodiscard]] int getType(int valueIdx) const override;

    void addType(int type) override;

    [[nodiscard]] int getIdx(const std::string &fieldName) const override;

public:
    static TempRecord getSortPaddingRecord();

    [[nodiscard]] BitSecret compareField(const TempRecord &other, const std::string &fieldName) const;

    static void muxSwap(TempRecord &first, TempRecord &second, BitSecret swap);

    static BitSecret requiresSwap(const TempRecord &r0, const TempRecord &r1,
                                  const std::vector<std::string> &orderFields,
                                  const std::vector<BitSecret> &ascendingOrders);

    static void bitonicSort(std::vector<TempRecord> &records,
                            const std::vector<std::string> &fieldNames,
                            const std::vector<BitSecret> &ascendingOrders);
};


#endif //TEMPRECORD_H
