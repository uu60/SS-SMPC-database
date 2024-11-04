//
// Created by 杜建璋 on 2024/11/3.
//

#include "operator/Insert.h"

#include <sstream>
#include <hsql/SQLParser.h>
#include "basis/Table.h"
#include "dbms/SystemManager.h"

bool Insert::clientInsert(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    int done;
    const auto *insertStmt = dynamic_cast<const hsql::InsertStatement *>(stmt);

    std::string tableName = insertStmt->tableName;
    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tableName);
    if (!table) {
        resp << "Failed. Table `" + tableName + "` does not exist." << std::endl;
        return false;
    }
    const auto fieldNames = table->fieldNames();

    // get inserted values
    const auto columns = insertStmt->columns;
    const auto values = insertStmt->values;
    std::vector<std::string> cols;

    // insert values all columns
    if (!insertStmt->columns) {
        cols = table->fieldNames();
    } else {
        for (auto c: *columns) {
            cols.emplace_back(c);
        }
    }

    if (cols.size() != values->size()) {
        resp << "Failed. Unmatched parameter numbers." << std::endl;
        return false;
    }
    for (auto &column: cols) {
        if (std::ranges::find(fieldNames, column) == fieldNames.end()) {
            resp << "Failed. Unknown field name `" + column + "`." << std::endl;
            return false;
        }
    }

    std::vector<int64_t> parsedValues;
    for (size_t i = 0; i < values->size(); ++i) {
        const auto expr = (*values)[i];
        int64_t v;

        if (expr->type == hsql::kExprLiteralInt) {
            v = expr->ival;
        } else if (expr->type == hsql::kExprOperator && expr->expr->type == hsql::kExprLiteralInt) {
            if (expr->opType == hsql::kOpUnaryMinus) {
                v = -expr->expr->ival;
            }
        } else {
            resp << "Failed. Unsupported value type." << std::endl;
            return false;
        }

        // column idx in the table
        int64_t idx = std::distance(fieldNames.begin(),
                                    std::find(fieldNames.begin(), fieldNames.end(), cols[i]));
        int type = table->fieldTypes()[idx];
        int64_t masked = v & ((1LL << type) - 1);
        if (type < 64 && masked != v) {
            resp << "Failed. Inserted parameters out of range." << std::endl;
            return false;
        }
        parsedValues.emplace_back(v);
    }

    json j;
    j["type"] = SystemManager::getCommandPrefix(SystemManager::INSERT);
    j["name"] = tableName;
    std::string m = j.dump();

    Comm::send(&m, 0);
    Comm::send(&m, 1);

    // secret share
    for (size_t i = 0; i < table->fieldTypes().size(); ++i) {
        int type = table->fieldTypes()[i];
        // table field idx in the inserted columns
        const auto &find = std::find(cols.begin(), cols.end(), fieldNames[i]);
        int64_t idx = std::distance(cols.begin(), find);

        int64_t v = find != cols.end() ? parsedValues[idx] : 0;
        if (type == 1) {
            BitSecret(v).share();
        } else if (type == 8) {
            IntSecret(static_cast<int8_t>(v)).share();
        } else if (type == 16) {
            IntSecret(static_cast<int16_t>(v)).share();
        } else if (type == 32) {
            IntSecret(static_cast<int32_t>(v)).share();
        } else {
            IntSecret(v).share();
        }
    }

    Comm::recv(&done, 0);
    Comm::recv(&done, 1);
    resp << "OK. Record inserted into `" + tableName + "`." << std::endl;

    return true;
}

void Insert::serverInsert(nlohmann::basic_json<> j) {
    std::string tbName = j.at("name").get<std::string>();
    Table *table = SystemManager::getInstance()._currentDatabase->getTable(tbName);

    std::vector<int> types = table->fieldTypes();
    TableRecord r(table);
    for (int t: types) {
        if (t == 1) {
            BitSecret s = BitSecret(false).share();
            r.addField(s, t);
        } else if (t == 8) {
            IntSecret s = IntSecret<int8_t>(0).share();
            r.addField(s, t);
        } else if (t == 16) {
            IntSecret s = IntSecret<int16_t>(0).share();
            r.addField(s, t);
        } else if (t == 32) {
            IntSecret s = IntSecret<int32_t>(0).share();
            r.addField(s, t);
        } else {
            IntSecret s = IntSecret<int64_t>(0).share();
            r.addField(s, t);
        }
    }
    table->insert(r);
}