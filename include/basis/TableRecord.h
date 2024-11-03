//
// Created by 杜建璋 on 2024/11/1.
//

#ifndef TABLERECORD_H
#define TABLERECORD_H
#include "./AbstractRecord.h"
#include "./TempRecord.h"

class Table;

class TableRecord : public AbstractRecord {
private:
    Table *_owner{};

public:
    explicit TableRecord(Table *owner);

    [[nodiscard]] Table *owner() const;

    [[nodiscard]] TempRecord convertToTemp() const;

protected:
    [[nodiscard]] int getType(int idx) const override;

    void addType(int type) override;

    [[nodiscard]] int getIdx(const std::string &fieldName) const override;
};



#endif //TABLERECORD_H
