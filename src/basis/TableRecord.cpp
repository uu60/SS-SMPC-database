//
// Created by 杜建璋 on 2024/11/1.
//

#include "../../include/basis/TableRecord.h"
#include "../../include/basis/Table.h"

TableRecord::TableRecord(Table *owner) {
    this->_owner = owner;
}

Table *TableRecord::owner() const {
    return _owner;
}

TempRecord TableRecord::convertToTemp() const {
    TempRecord t;
    t._fieldValues = _fieldValues;
    t._fieldNames = _owner->fieldNames();
    t._types = _owner->fieldTypes();
    return t;
}

int TableRecord::getType(int idx) const {
    return _owner->fieldTypes()[idx];
}

void TableRecord::addType(int type) {
    // do nothing
}

int TableRecord::getIdx(const std::string &fieldName) const {
    for (int i = 0; i < _owner->fieldNames().size(); i++) {
        if (_owner->fieldNames()[i] == fieldName) {
            return i;
        }
    }
    return -1;
}


