//
// Created by 杜建璋 on 2024/11/3.
//

#include "operator/Select.h"
#include <mpc_package/utils/Log.h>
#include "basis/Table.h"
#include <nlohmann/json.hpp>

#include "dbms/SystemManager.h"
#include "function/Order.h"
using json = nlohmann::json;

bool Select::clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    int done;

    const auto *selectStmt = dynamic_cast<const hsql::SelectStatement *>(stmt);

    std::string tableName = selectStmt->fromTable->getName();

    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
    if (!table) {
        resp << "Failed. Table `" << tableName << "` does not exist." << std::endl;
        return false;
    }
    const auto fieldNames = table->fieldNames();

    // select content
    std::vector<std::string> selectedFieldNames;
    const auto list = selectStmt->selectList;
    for (const auto c: *list) {
        // select *
        if (c->type == hsql::kExprStar) {
            selectedFieldNames = table->fieldNames();
            break;
        }
        if (c->type == hsql::kExprColumnRef) {
            selectedFieldNames.emplace_back(c->getName());
        }
    }

    // order
    std::vector<std::string> orderFields;
    std::vector<bool> ascendings;
    if (selectStmt->order) {
        const auto *order = selectStmt->order;
        for (auto desc: *order) {
            auto name = desc->expr->getName();
            if (std::ranges::find(fieldNames, name) == fieldNames.end()) {
                resp << "Failed. Table does not have field `" << name << "`." << std::endl;
                return false;
            }
            orderFields.emplace_back(name);
            bool isAscending = desc->type == hsql::kOrderAsc;
            ascendings.emplace_back(isAscending);
        }
    }

    // notify servers
    json j;
    j["type"] = SystemManager::getCommandPrefix(SystemManager::SELECT);
    j["name"] = tableName;
    j["fieldNames"] = selectedFieldNames;
    if (selectStmt->order) {
        j["orderFields"] = orderFields;
        j["ascendings"] = ascendings;
    }
    std::string m = j.dump();
    Comm::send(&m, 0);
    Comm::send(&m, 1);

    std::vector<TempRecord> rawRecords;
    int64_t c;
    Comm::recv(&c, 0);
    auto count = static_cast<uint64_t>(c);

    for (int i = 0; i < count; i++) {
        TempRecord temp;
        for (const auto &selectedField: selectedFieldNames) {
            // column idx in the table
            int64_t idx = std::distance(fieldNames.begin(),
                                        std::ranges::find(fieldNames, selectedField));
            int type = table->fieldTypes()[idx];
            if (type == 1) {
                temp.addField(BitSecret(false).reconstruct(), type);
            } else if (type == 8) {
                temp.addField(IntSecret<int8_t>(0).reconstruct(), type);
            } else if (type == 16) {
                temp.addField(IntSecret<int16_t>(0).reconstruct(), type);
            } else if (type == 32) {
                temp.addField(IntSecret<int32_t>(0).reconstruct(), type);
            } else {
                temp.addField(IntSecret<int64_t>(0).reconstruct(), type);
            }
        }

        BitSecret b = BitSecret(false).reconstruct();
        if (b.get()) {
            rawRecords.push_back(temp);
        }
    }

    for (const auto &field: table->fieldNames()) {
        resp << std::setw(10) << field;
    }
    resp << std::endl;

    for (const auto &record: rawRecords) {
        record.print(resp);
    }

    Comm::recv(&done, 0);
    Comm::recv(&done, 1);
    return true;
}

void Select::serverSelect(json j) {
    std::string tableName = j.at("name").get<std::string>();
    std::vector<std::string> selectedFields = j.at("fieldNames").get<std::vector<std::string> >();

    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
    std::vector<TempRecord> records = table->selectAll();

    // order
    if (j.contains("orderFields")) {
        std::vector<std::string> orderFields = j.at("orderFields").get<std::vector<std::string> >();
        std::vector<bool> ascendings = j.at("ascendings").get<std::vector<bool> >();

        std::vector<BitSecret> ascs;
        ascs.reserve(ascendings.size());

        for (bool a: ascendings) {
            ascs.emplace_back(a & Comm::rank());
        }
        Order::bitonicSort(records, orderFields, ascs);
    }

    auto count = static_cast<int64_t>(records.size());
    if (Comm::rank() == 0) {
        Comm::send(&count, Comm::CLIENT_RANK);
    }

    const auto fieldNames = table->fieldNames();
    for (int i = 0; i < count; i++) {
        TempRecord &cur = records[i];

        for (const auto &selectedField: selectedFields) {
            // column index in the table
            int64_t idx = std::distance(fieldNames.begin(),
                                        std::ranges::find(fieldNames, selectedField));
            int t = table->fieldTypes()[idx];
            if (t == 1) {
                std::get<BitSecret>(cur._fieldValues[idx]).reconstruct();
            } else if (t == 8) {
                std::get<IntSecret<int8_t> >(cur._fieldValues[idx]).reconstruct();
            } else if (t == 16) {
                std::get<IntSecret<int16_t> >(cur._fieldValues[idx]).reconstruct();
            } else if (t == 32) {
                std::get<IntSecret<int32_t> >(cur._fieldValues[idx]).reconstruct();
            } else {
                std::get<IntSecret<int64_t> >(cur._fieldValues[idx]).reconstruct();
            }
        }

        cur._valid.reconstruct();
    }
}
