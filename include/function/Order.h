//
// Created by 杜建璋 on 2024/11/3.
//

#ifndef ORDER_H
#define ORDER_H
#include "basis/TempRecord.h"

class Order {
public:
    static void muxSwap(TempRecord &first, TempRecord &second, BitSecret swap);

    static BitSecret requiresSwap(const TempRecord &r0, const TempRecord &r1,
                                  const std::vector<std::string> &orderFields,
                                  const std::vector<BitSecret> &ascendingOrders);

    static void bitonicSort(std::vector<TempRecord> &records,
                            const std::vector<std::string> &fieldNames,
                            const std::vector<BitSecret> &ascendingOrders);
};



#endif //ORDER_H
