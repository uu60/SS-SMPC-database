//
// Created by 杜建璋 on 2024/10/25.
//

#include "dbms/DBMS.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <limits>
#include <iomanip>
#include <mpc_package/utils/Log.h>
#include <SQLParser.h>
#include <sql/SQLStatement.h>
#include <nlohmann/json.hpp>
#include "socket/LocalServer.h"

using json = nlohmann::json;

const std::string DBMS::INS_PRE = "$ins$";
const std::string DBMS::SEL_PRE = "$sel$";


DBMS &DBMS::getInstance() {
    static DBMS instance;
    return instance;
}

bool DBMS::createDatabase(const std::string &dbName, std::string &msg) {
    if (_databases.find(dbName) != _databases.end()) {
        msg = "Database " + dbName + " already exists.";
        return false;
    }
    _databases[dbName] = Database(dbName);
    return true;
}

bool DBMS::dropDatabase(const std::string &dbName, std::string &msg) {
    if (_currentDatabase && _currentDatabase->name() == dbName) {
        _currentDatabase = nullptr;
    }
    if (_databases.erase(dbName) > 0) {
        return true;
    }
    msg = "Database " + dbName + " does not exist.";
    return false;
}

bool DBMS::useDatabase(const std::string &dbName, std::string &msg) {
    auto it = _databases.find(dbName);
    if (it != _databases.end()) {
        _currentDatabase = &(it->second);
        return true;
    }
    msg = "Database " + dbName + " does not exist.";
    return false;
}

std::string handleEndingDatabaseName(std::istringstream &iss) {
    std::string dbName;
    iss >> dbName;
    // missing db name
    if (dbName.empty()) {
        DBMS::log("Syntax error: Missing database name.", false);
        return "";
    }

    // remove extra chars
    std::string remaining;
    std::getline(iss, remaining);
    remaining.erase(std::remove_if(remaining.begin(), remaining.end(), ::isspace), remaining.end());

    // handle `;`
    if (!remaining.empty() && remaining != ";") {
        DBMS::log("Syntax error: Invalid characters after database name.", false);
        return "";
    }
    if (dbName.back() == ';') {
        dbName.pop_back();
    }

    return dbName;
}

void DBMS::notifyServersSync(json &j) {
    int done;
    std::string m = j.dump();
    Comm::send(&m, 0);
    Comm::send(&m, 1);

    // sync
    Comm::recv(&done, 0);
    Comm::recv(&done, 1);
}

void DBMS::execute(const std::string &command) {
    std::istringstream iss(command);
    std::string word;
    iss >> word;
    // handle `create db` and `use db`
    bool create = strcasecmp(word.c_str(), "create") == 0;
    bool drop = strcasecmp(word.c_str(), "drop") == 0;
    if (create || drop) {
        iss >> word;
        if (strcasecmp(word.c_str(), "database") == 0) {
            std::string dbName = handleEndingDatabaseName(iss);

            if (create) {
                // execute
                if (!createDatabase(dbName, msg)) {
                    log(msg, false);
                    handleFailedExecution();
                    return;
                }
                // notify servers
                json j;
                j["type"] = "cdb";
                j["name"] = dbName;
                notifyServersSync(j);

                log("Database `" + dbName + "` created.", true);
            } else {
                if (!dropDatabase(dbName, msg)) {
                    log(msg, false);
                    handleFailedExecution();
                }

                // notify servers
                json j;
                j["type"] = "ddb";
                j["name"] = dbName;
                notifyServersSync(j);

                log("Database " + dbName + " dropped.", true);
            }
            return;
        }
    }

    if (strcasecmp(word.c_str(), "use") == 0) {
        std::string dbName = handleEndingDatabaseName(iss);
        if (!useDatabase(dbName, msg)) {
            log(msg, false);
            handleFailedExecution();
        }
        // notify servers
        json j;
        j["type"] = "udb";
        j["name"] = dbName;
        notifyServersSync(j);

        log("Database `" + dbName + "` selected.", true);
        return;
    }

    if (!_currentDatabase) {
        log("No database selected.", false);
        handleFailedExecution();
        return;
    }

    hsql::SQLParserResult result;
    hsql::SQLParser::parse(command, &result);

    if (!result.isValid()) {
        DBMS::log(result.errorMsg(), false);
        handleFailedExecution();
        return;
    }

    std::ostringstream resp;
    for (int si = 0; si < result.getStatements().size(); si++) {
        auto stmt = result.getStatement(si);
        switch (stmt->type()) {
            case hsql::kStmtCreate: {
                const auto *createStmt = dynamic_cast<const hsql::CreateStatement *>(stmt);
                if (createStmt->type != hsql::kCreateTable) {
                    resp << "Unsupported CREATE statement type." << std::endl;
                    handleFailedExecution();
                    goto over;
                }

                std::string tableName = createStmt->tableName;

                if (_currentDatabase->getTable(tableName)) {
                    resp << "Failed. Table `" + tableName + "` already exists." << std::endl;
                    handleFailedExecution();
                    goto over;
                }

                std::vector<std::string> fieldNames;
                std::vector<int> fieldTypes;

                for (const auto *column: *createStmt->columns) {
                    std::string fieldName = column->name;

                    int64_t type;
                    switch (column->type.data_type) {
                        case hsql::DataType::BOOLEAN:
                            type = 1;
                            break;
                        case hsql::DataType::SMALLINT:
                            type = 16;
                            break;
                        case hsql::DataType::INT:
                            type = 32;
                            break;
                        case hsql::DataType::LONG:
                            type = 64;
                            break;
                        default:
                            resp << "Failed. Unsupported data type for field `" + fieldName + "`." << std::endl;
                            handleFailedExecution();
                            goto over;
                    }

                    fieldNames.push_back(fieldName);
                    fieldTypes.push_back((int32_t) type);
                }

                if (!_currentDatabase->createTable(tableName, fieldNames, fieldTypes, msg)) {
                    resp << "Failed. " << msg << std::endl;
                    handleFailedExecution();
                    goto over;
                }
                // notify servers
                json j;
                j["type"] = "ctb";
                j["name"] = tableName;
                j["fieldNames"] = fieldNames;
                j["fieldTypes"] = fieldTypes;
                notifyServersSync(j);

                resp << "OK. Table `" + tableName + "` created.";
                break;
            }
            case hsql::kStmtDrop: {
                const auto *dropStmt = dynamic_cast<const hsql::DropStatement *>(stmt);

                if (dropStmt->type == hsql::kDropTable) {
                    std::string tableName = dropStmt->name;
                    std::cout << "Dropping table: " << tableName << std::endl;

                    if (!_currentDatabase->dropTable(tableName, msg)) {
                        resp << "Failed " << msg << std::endl;
                        goto over;
                    }

                    // notify servers
                    json j;
                    j["type"] = "dtb";
                    j["name"] = tableName;
                    notifyServersSync(j);

                    resp << "OK. Table " + tableName + " dropped successfully." << std::endl;
                    break;
                } else {
                    resp << "Failed. Unsupported DROP statement type." << std::endl;
                    handleFailedExecution();
                    goto over;
                }
            }
            case hsql::kStmtInsert: {
                const auto *insertStmt = dynamic_cast<const hsql::InsertStatement *>(stmt);

                std::string tableName = insertStmt->tableName;
                Table *table = _currentDatabase->getTable(tableName);
                if (!table) {
                    resp << "Failed. Table `" + tableName + "` does not exist." << std::endl;
                    handleFailedExecution();
                    goto over;
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
                    handleFailedExecution();
                    goto over;;
                }
                for (auto &column: cols) {
                    if (std::find(fieldNames.begin(), fieldNames.end(), column) == fieldNames.end()) {
                        resp << "Failed. Unknown field name `" + column + "`." << std::endl;
                        handleFailedExecution();
                        goto over;
                    }
                }

                std::vector<int64_t> parsedValues;
                for (size_t i = 0; i < values->size(); ++i) {
                    const auto expr = (*values)[i];
                    int64_t v;

                    if (expr->type == hsql::kExprLiteralInt) {
                        v = expr->ival;
                    } else {
                        resp << "Failed. Unsupported value type." << std::endl;
                        handleFailedExecution();
                        goto over;
                    }

                    // column idx in the table
                    int64_t idx = std::distance(fieldNames.begin(),
                                                std::find(fieldNames.begin(), fieldNames.end(), cols[i]));
                    int type = table->fieldTypes()[idx];
                    int64_t masked = v & ((1LL << type) - 1);
                    if (type < 64 && masked != v) {
                        resp << "Failed. Inserted parameters out of range." << std::endl;
                        handleFailedExecution();
                        goto over;
                    }
                    parsedValues.emplace_back(v);
                }

                json j;
                j["type"] = "ins";
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
                        IntSecret<int8_t>(static_cast<int8_t>(v)).share();
                    } else if (type == 16) {
                        IntSecret<int16_t>(static_cast<int16_t>(v)).share();
                    } else if (type == 32) {
                        IntSecret<int32_t>(static_cast<int32_t>(v)).share();
                    } else {
                        IntSecret<int64_t>(v).share();
                    }
                }

                Comm::recv(&done, 0);
                Comm::recv(&done, 1);
                resp << "OK. Record inserted into " + tableName + "." << std::endl;

                break;
            }
            case hsql::kStmtSelect: {
                const auto *selectStmt = dynamic_cast<const hsql::SelectStatement *>(stmt);

                std::string tableName = selectStmt->fromTable->getName();

                Table *table = _currentDatabase->getTable(tableName);
                if (!table) {
                    resp << "Failed. Table `" + tableName + "` does not exist." << std::endl;
                    handleFailedExecution();
                    goto over;
                }
                const auto fieldNames = table->fieldNames();

                // select content
                std::vector<std::string> selectedFields;
                const auto list = selectStmt->selectList;
                for (const auto c: *list) {
                    // select *
                    if (c->type == hsql::kExprStar) {
                        selectedFields = table->fieldNames();
                        break;
                    }
                    if (c->type == hsql::kExprColumnRef) {
                        selectedFields.emplace_back(c->getName());
                    }
                }

                // order
                if (!selectStmt->order) {
                    const auto* order = selectStmt->order;

                }

                // notify servers
                json j;
                j["type"] = "sel";
                j["name"] = tableName;
                j["fieldNames"] = selectedFields;
                std::string m = j.dump();
                Comm::send(&m, 0);
                Comm::send(&m, 1);

                std::vector<Record> rawRecords;
                int64_t c;
                Comm::recv(&c, 0);
                auto count = static_cast<uint64_t>(c);

                for (int i = 0; i < count; i++) {
                    Record temp;
                    for (const auto & selectedField : selectedFields) {
                        // column idx in the table
                        int64_t idx = std::distance(fieldNames.begin(),
                                                    std::find(fieldNames.begin(), fieldNames.end(), selectedField));
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
                    rawRecords.push_back(temp);
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
                break;
            }
            default: {
                resp << "Syntax error." << std::endl;
                handleFailedExecution();
                goto over;
            }
        }
    }
    resp << std::endl;
    over: LocalServer::getInstance().send_(resp.str());
}

void DBMS::log(const std::string &msg, bool success) {
    LocalServer::getInstance().send_((success ? "OK. " : "Failed. ") + msg + "\n");
}

void DBMS::handleRequests() {
    while (true) {
        std::string jstr;
        Comm::recv(&jstr, Comm::CLIENT_RANK);
        auto j = json::parse(jstr);
        std::string type = j.at("type").get<std::string>();

        if (type == "exit") {
            Comm::send(&done, Comm::CLIENT_RANK);
            break;
        } else if (type == "cdb") { // create database
            std::string dbName = j.at("name").get<std::string>();
            createDatabase(dbName, msg);
        } else if (type == "ddb") {
            std::string dbName = j.at("name").get<std::string>();
            dropDatabase(dbName, msg);
        } else if (type == "udb") {
            std::string dbName = j.at("name").get<std::string>();
            useDatabase(dbName, msg);
        } else if (type == "ctb") {
            std::string tbName = j.at("name").get<std::string>();
            std::vector<std::string> fieldNames = j.at("fieldNames").get<std::vector<std::string>>();
            std::vector<int32_t> fieldTypes = j.at("fieldTypes").get<std::vector<int32_t>>();
            _currentDatabase->createTable(tbName, fieldNames, fieldTypes, msg);
        } else if (type == "dtb") {
            std::string tbName = j.at("name").get<std::string>();
            _currentDatabase->dropTable(tbName, msg);
        } else if (type == "ins") {
            std::string tbName = j.at("name").get<std::string>();
            Table *table = _currentDatabase->getTable(tbName);

            std::vector<int> types = table->fieldTypes();
            Record r(table);
            for (int t : types) {
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
        } else if (type == "sel") {
            std::string tableName = j.at("name").get<std::string>();
            std::vector<std::string> selectedFields = j.at("fieldNames").get<std::vector<std::string>>();

            Table *table = _currentDatabase->getTable(tableName);
            std::vector<Record> records = table->allRecords();
            auto count = (int64_t) (records.size());
            if (Comm::rank() == 0) {
                Comm::send(&count, Comm::CLIENT_RANK);
            }

            const auto fieldNames = table->fieldNames();
            for (int i = 0; i < count; i++) {
                const Record &cur = records[i];

                for (const auto & selectedField : selectedFields) {
                    // column idx in the table
                    int64_t idx = std::distance(fieldNames.begin(),
                                                std::find(fieldNames.begin(), fieldNames.end(), selectedField));
                    int type = table->fieldTypes()[idx];
                    if (type == 1) {
                        std::get<BitSecret>(cur.fieldValues()[idx]).reconstruct();
                    } else if (type == 8) {
                        std::get<IntSecret<int8_t>>(cur.fieldValues()[idx]).reconstruct();
                    } else if (type == 16) {
                        std::get<IntSecret<int16_t>>(cur.fieldValues()[idx]).reconstruct();
                    } else if (type == 32) {
                        std::get<IntSecret<int32_t>>(cur.fieldValues()[idx]).reconstruct();
                    } else {
                        std::get<IntSecret<int64_t>>(cur.fieldValues()[idx]).reconstruct();
                    }
                }
            }
        }
        // sync
        Comm::send(&done, Comm::CLIENT_RANK);
    }
}

void DBMS::handleFailedExecution() {

}

Database *DBMS::currentDatabase() {
    return _currentDatabase;
}
