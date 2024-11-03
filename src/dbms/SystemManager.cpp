//
// Created by 杜建璋 on 2024/10/25.
//

#include "dbms/SystemManager.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <limits>
#include <iomanip>
#include <SQLParser.h>
#include <mpc_package/utils/Log.h>
#include <sql/SQLStatement.h>
#include <nlohmann/json.hpp>

#include "basis/TableRecord.h"
#include "basis/TempRecord.h"
#include "socket/LocalServer.h"
#include <mpc_package/utils/System.h>

using json = nlohmann::json;

enum CommandType {
    EXIT,
    CREATE_DB,
    DROP_DB,
    USE_DB,
    CREATE_TABLE,
    DROP_TABLE,
    INSERT,
    SELECT,
    UNKNOWN
};

CommandType getCommandType(const std::string &prefix) {
    static const std::unordered_map<std::string, CommandType> typeMap = {
        {"exit", EXIT},
        {"cdb", CREATE_DB},
        {"ddb", DROP_DB},
        {"udb", USE_DB},
        {"ctb", CREATE_TABLE},
        {"dtb", DROP_TABLE},
        {"ins", INSERT},
        {"sel", SELECT}
    };
    auto it = typeMap.find(prefix);
    return (it != typeMap.end()) ? it->second : UNKNOWN;
}

std::string getCommandPrefix(const CommandType type) {
    static const std::unordered_map<CommandType, std::string> typeMap = {
        {EXIT, "exit"},
        {CREATE_DB, "cdb"},
        {DROP_DB, "ddb"},
        {USE_DB, "udb"},
        {CREATE_TABLE, "ctb"},
        {DROP_TABLE, "dtb"},
        {INSERT, "ins"},
        {SELECT, "sel"}
    };
    auto it = typeMap.find(type);
    return (it != typeMap.end()) ? it->second : "exit";
}

SystemManager &SystemManager::getInstance() {
    static SystemManager instance;
    return instance;
}

bool SystemManager::createDatabase(const std::string &dbName, std::string &msg) {
    if (_databases.find(dbName) != _databases.end()) {
        msg = "Database " + dbName + " already exists.";
        return false;
    }
    _databases[dbName] = Database(dbName);
    return true;
}

bool SystemManager::dropDatabase(const std::string &dbName, std::string &msg) {
    if (_currentDatabase && _currentDatabase->name() == dbName) {
        _currentDatabase = nullptr;
    }
    if (_databases.erase(dbName) > 0) {
        return true;
    }
    msg = "Database " + dbName + " does not exist.";
    return false;
}

bool SystemManager::useDatabase(const std::string &dbName, std::string &msg) {
    auto it = _databases.find(dbName);
    if (it != _databases.end()) {
        _currentDatabase = &(it->second);
        return true;
    }
    msg = "Database " + dbName + " does not exist.";
    return false;
}

std::string handleEndingDatabaseName(std::istringstream &iss, std::ostringstream &resp) {
    std::string dbName;
    iss >> dbName;
    // missing db name
    if (dbName.empty()) {
        resp << "Failed. Syntax error: Missing database name." << std::endl;
        return "";
    }

    // remove extra chars
    std::string remaining;
    std::getline(iss, remaining);
    remaining.erase(std::remove_if(remaining.begin(), remaining.end(), ::isspace), remaining.end());

    // handle `;`
    if (!remaining.empty() && remaining != ";") {
        resp << "Failed. Syntax error: Invalid characters after database name." << std::endl;
        return "";
    }
    if (dbName.back() == ';') {
        dbName.pop_back();
    }

    return dbName;
}

void SystemManager::notifyServersSync(json &j) {
    int done;
    std::string m = j.dump();
    Comm::send(&m, 0);
    Comm::send(&m, 1);

    // sync
    Comm::recv(&done, 0);
    Comm::recv(&done, 1);
}

bool SystemManager::clientCreateTable(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    const auto *createStmt = dynamic_cast<const hsql::CreateStatement *>(stmt);
    if (createStmt->type != hsql::kCreateTable) {
        resp << "Unsupported CREATE statement type." << std::endl;
        handleFailedExecution();
        return false;
    }

    std::string tableName = createStmt->tableName;

    if (_currentDatabase->getTable(tableName)) {
        resp << "Failed. Table `" + tableName + "` already exists." << std::endl;
        handleFailedExecution();
        return false;
    }

    std::vector<std::string> fieldNames;
    std::vector<int> fieldTypes;

    for (const auto *column: *createStmt->columns) {
        std::string fieldName = column->name;

        int type;
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
                return false;
        }

        fieldNames.push_back(fieldName);
        fieldTypes.push_back(type);
    }

    if (!_currentDatabase->createTable(tableName, fieldNames, fieldTypes, msg)) {
        resp << "Failed. " << msg << std::endl;
        handleFailedExecution();
        return false;
    }

    // notify servers
    json j;
    j["type"] = getCommandPrefix(CREATE_TABLE);
    j["name"] = tableName;
    j["fieldNames"] = fieldNames;
    j["fieldTypes"] = fieldTypes;
    notifyServersSync(j);

    resp << "OK. Table `" + tableName + "` created." << std::endl;
    return true;
}

bool SystemManager::clientDropTable(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    const auto *dropStmt = dynamic_cast<const hsql::DropStatement *>(stmt);

    if (dropStmt->type == hsql::kDropTable) {
        std::string tableName = dropStmt->name;
        std::cout << "Dropping table: " << tableName << std::endl;

        if (!_currentDatabase->dropTable(tableName, msg)) {
            resp << "Failed " << msg << std::endl;
            return false;
        }

        // notify servers
        json j;
        j["type"] = getCommandPrefix(DROP_TABLE);
        j["name"] = tableName;
        notifyServersSync(j);

        resp << "OK. Table " + tableName + " dropped successfully." << std::endl;
        return true;
    }
    resp << "Failed. Unsupported DROP statement type." << std::endl;
    handleFailedExecution();
    return false;
}

// return if is create table
bool SystemManager::clientCreateDeleteDb(std::istringstream &iss, std::ostringstream &resp, std::string &word,
                                         bool create) {
    iss >> word;
    if (strcasecmp(word.c_str(), "database") == 0) {
        std::string dbName = handleEndingDatabaseName(iss, resp);

        if (dbName.empty()) {
            handleFailedExecution();
            return false;
        }

        if (create) {
            // execute
            if (!createDatabase(dbName, msg)) {
                resp << "Failed. " << msg << std::endl;
                handleFailedExecution();
                return false;
            }
            // notify servers
            json j;
            j["type"] = getCommandPrefix(CREATE_DB);
            j["name"] = dbName;
            notifyServersSync(j);

            resp << "OK. Database `" + dbName + "` created." << std::endl;
        } else {
            if (!dropDatabase(dbName, msg)) {
                resp << "Failed. " << msg << std::endl;
                handleFailedExecution();
                return false;
            }

            // notify servers
            json j;
            j["type"] = getCommandPrefix(DROP_DB);
            j["name"] = dbName;
            notifyServersSync(j);

            resp << "OK. Database " + dbName + " dropped." << std::endl;
        }
        return false;
    }
    return true;
}

void SystemManager::clientUseDb(std::istringstream &iss, std::ostringstream &resp) {
    std::string dbName = handleEndingDatabaseName(iss, resp);
    if (!useDatabase(dbName, msg)) {
        resp << "Failed. " << msg << std::endl;
        handleFailedExecution();
        return;
    }
    // notify servers
    json j;
    j["type"] = getCommandPrefix(USE_DB);
    j["name"] = dbName;
    notifyServersSync(j);

    resp << "OK. Database `" + dbName + "` selected." << std::endl;
}

bool SystemManager::clientInsert(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    const auto *insertStmt = dynamic_cast<const hsql::InsertStatement *>(stmt);

    std::string tableName = insertStmt->tableName;
    Table *table = _currentDatabase->getTable(tableName);
    if (!table) {
        resp << "Failed. Table `" + tableName + "` does not exist." << std::endl;
        handleFailedExecution();
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
        handleFailedExecution();
        return false;
    }
    for (auto &column: cols) {
        if (std::ranges::find(fieldNames, column) == fieldNames.end()) {
            resp << "Failed. Unknown field name `" + column + "`." << std::endl;
            handleFailedExecution();
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
            handleFailedExecution();
            return false;
        }

        // column idx in the table
        int64_t idx = std::distance(fieldNames.begin(),
                                    std::find(fieldNames.begin(), fieldNames.end(), cols[i]));
        int type = table->fieldTypes()[idx];
        int64_t masked = v & ((1LL << type) - 1);
        if (type < 64 && masked != v) {
            resp << "Failed. Inserted parameters out of range." << std::endl;
            handleFailedExecution();
            return false;
        }
        parsedValues.emplace_back(v);
    }

    json j;
    j["type"] = getCommandPrefix(INSERT);
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

bool SystemManager::clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt) {
    const auto *selectStmt = dynamic_cast<const hsql::SelectStatement *>(stmt);

    std::string tableName = selectStmt->fromTable->getName();

    Table *table = _currentDatabase->getTable(tableName);
    if (!table) {
        resp << "Failed. Table `" << tableName << "` does not exist." << std::endl;
        handleFailedExecution();
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
                handleFailedExecution();
                return false;
            }
            orderFields.emplace_back(name);
            bool isAscending = desc->type == hsql::kOrderAsc;
            ascendings.emplace_back(isAscending);
        }
    }

    // notify servers
    json j;
    j["type"] = getCommandPrefix(SELECT);
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

void SystemManager::execute(const std::string &command) {
    int64_t start = System::currentTimeMillis();
    std::istringstream iss(command);
    std::string word;
    iss >> word;
    std::ostringstream resp;
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(command, &result);
    // handle `create db` and `use db`
    bool create = strcasecmp(word.c_str(), "create") == 0;
    bool drop = strcasecmp(word.c_str(), "drop") == 0;
    if (create || drop) {
        // return if create table
        if (!clientCreateDeleteDb(iss, resp, word, create)) goto over;
    }

    if (strcasecmp(word.c_str(), "use") == 0) {
        clientUseDb(iss, resp);
        goto over;
    }

    if (!_currentDatabase) {
        resp << "Failed. No database selected." << std::endl;
        handleFailedExecution();
        goto over;
    }

    if (!result.isValid()) {
        resp << "Failed. " << result.errorMsg() << std::endl;
        handleFailedExecution();
        goto over;
    }

    for (int si = 0; si < result.getStatements().size(); si++) {
        auto stmt = result.getStatement(si);
        switch (stmt->type()) {
            case hsql::kStmtCreate: {
                if (!clientCreateTable(resp, stmt)) goto over;
                break;
            }
            case hsql::kStmtDrop: {
                if (!clientDropTable(resp, stmt)) goto over;
                break;
            }
            case hsql::kStmtInsert: {
                if (!clientInsert(resp, stmt)) goto over;
                break;
            }
            case hsql::kStmtSelect: {
                if (!clientSelect(resp, stmt)) goto over;
                break;
            }
            default: {
                resp << "Syntax error." << std::endl;
                handleFailedExecution();
                goto over;
            }
        }
    }
over:
    resp << "(" << System::currentTimeMillis() - start << " ms)" << std::endl;
    LocalServer::getInstance().send_(resp.str());
}

void SystemManager::log(const std::string &msg, bool success) {
    LocalServer::getInstance().send_((success ? "OK. " : "Failed. ") + msg + "\n");
}

void SystemManager::serverSelect(nlohmann::basic_json<> j) const {
    std::string tableName = j.at("name").get<std::string>();
    std::vector<std::string> selectedFields = j.at("fieldNames").get<std::vector<std::string> >();

    Table *table = _currentDatabase->getTable(tableName);
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
        TempRecord::bitonicSort(records, orderFields, ascs);
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

void SystemManager::serverInsert(nlohmann::basic_json<> j) {
    std::string tbName = j.at("name").get<std::string>();
    Table *table = _currentDatabase->getTable(tbName);

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

void SystemManager::handleRequests() {
    while (true) {
        std::string jstr;
        Comm::recv(&jstr, Comm::CLIENT_RANK);
        auto j = json::parse(jstr);
        std::string type = j.at("type").get<std::string>();
        CommandType commandType = getCommandType(type);

        switch (commandType) {
            case EXIT: {
                Comm::send(&done, Comm::CLIENT_RANK);
                break;
            }
            case CREATE_DB: {
                // create database
                std::string dbName = j.at("name").get<std::string>();
                createDatabase(dbName, msg);
                break;
            }
            case DROP_DB: {
                std::string dbName = j.at("name").get<std::string>();
                dropDatabase(dbName, msg);
                break;
            }
            case USE_DB: {
                std::string dbName = j.at("name").get<std::string>();
                useDatabase(dbName, msg);
                break;
            }
            case CREATE_TABLE: {
                std::string tbName = j.at("name").get<std::string>();
                std::vector<std::string> fieldNames = j.at("fieldNames").get<std::vector<std::string> >();
                std::vector<int32_t> fieldTypes = j.at("fieldTypes").get<std::vector<int32_t> >();
                _currentDatabase->createTable(tbName, fieldNames, fieldTypes, msg);
                break;
            }
            case DROP_TABLE: {
                std::string tbName = j.at("name").get<std::string>();
                _currentDatabase->dropTable(tbName, msg);
                break;
            }
            case INSERT: {
                serverInsert(j);
                break;
            }
            case SELECT: {
                serverSelect(j);
                break;
            }
            case UNKNOWN: {
                std::cerr << "Unknown command type: " << type << std::endl;
                break;
            }
        }
        // sync
        Comm::send(&done, Comm::CLIENT_RANK);
    }
}

void SystemManager::handleFailedExecution() {
}

Database *SystemManager::currentDatabase() {
    return _currentDatabase;
}
