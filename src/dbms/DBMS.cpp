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

const std::string DBMS::CDB_PRE = "$cdb$";
const std::string DBMS::DDB_PRE = "$ddb$";
const std::string DBMS::USE_PRE = "$use$";
const std::string DBMS::CTB_PRE = "$ctb$";
const std::string DBMS::DTB_PRE = "$dtb$";
const std::string DBMS::INS_PRE = "$ins$";
const std::string DBMS::SEL_PRE = "$sel$";


DBMS &DBMS::getInstance() {
    static DBMS instance;
    return instance;
}

bool DBMS::createDatabase(const std::string &dbName) {
    if (databases.find(dbName) != databases.end()) {
        log("Database " + dbName + " already exists.", false);
        return false;
    }
    databases[dbName] = Database(dbName);
    log("Database " + dbName + " created.", true);
    return true;
}

bool DBMS::deleteDatabase(const std::string &dbName) {
    if (currentDatabase && currentDatabase->getName() == dbName) {
        currentDatabase = nullptr;
    }
    if (databases.erase(dbName) > 0) {
        log("Database " + dbName + " deleted.", true);
        return true;
    }
    log("Database " + dbName + " does not exist.", false);
    return false;
}

bool DBMS::useDatabase(const std::string &dbName) {
    auto it = databases.find(dbName);
    if (it != databases.end()) {
        currentDatabase = &(it->second);
        log("Switched to database " + dbName + ".", true);
        return true;
    }
    log("Database " + dbName + " does not exist.", false);
    return false;
}

void DBMS::executeSQL(const std::string &command) {
    std::istringstream iss(command);
    std::string word;
    iss >> word;
    int done;

    if (word == "create") {
        iss >> word;
        if (word == "database") {
            std::string dbName;
            iss >> dbName;
            createDatabase(dbName);
            std::string exec = CDB_PRE + dbName;
            Comm::send(&exec, 0);
            Comm::send(&exec, 1);

            Comm::recv(&done, 0);
            Comm::recv(&done, 1);
        } else if (word == "table") {
            if (!currentDatabase) {
                log("Select a database first.", false);
                throw std::runtime_error("");
            }
            std::string exec = CTB_PRE + command.substr(std::string("create table").length());
            Comm::send(&exec, 0);
            Comm::send(&exec, 1);
            if (!currentDatabase) {
                log("Select a database first.", false);
                throw std::runtime_error("");
            }

            createTableByCommand(iss);
            Comm::recv(&done, 0);
            Comm::recv(&done, 1);
        } else {
            log("Invalid CREATE syntax.", false);
            throw std::runtime_error("");
        }

    } else if (word == "drop") {
        iss >> word;
        if (word == "database") {
            std::string dbName;
            iss >> dbName;
            deleteDatabase(dbName);
            std::string exec = DDB_PRE + dbName;
            Comm::send(&exec, 0);
            Comm::send(&exec, 1);

            Comm::recv(&done, 0);
            Comm::recv(&done, 1);
        } else if (word == "table") {
            if (!currentDatabase) {
                log("Select a database first.", false);
                throw std::runtime_error("");
            }
            std::string tableName;
            iss >> tableName;
            std::string msg;
            if (currentDatabase->deleteTable(tableName, msg)) {
                log("Table " + tableName + " deleted.", true);
            } else {
                std::cout << msg << std::endl;
            }
            std::string exec = DTB_PRE + tableName;
            Comm::send(&exec, 0);
            Comm::send(&exec, 1);

            Comm::recv(&done, 0);
            Comm::recv(&done, 1);
        } else {
            log("Invalid DROP syntax.", false);
            throw std::runtime_error("");
        }

    } else if (word == "use") {
        std::string dbName;
        iss >> dbName;
        std::string exec = USE_PRE + dbName;
        useDatabase(dbName);
        Comm::send(&exec, 0);
        Comm::send(&exec, 1);

        Comm::recv(&done, 0);
        Comm::recv(&done, 1);
    } else if (word == "select") {
        if (!currentDatabase) {
            log("Select a database first.", false);
            throw std::runtime_error("");
        }
        iss >> word;
        iss >> word;
        std::string tableName;
        iss >> tableName;

        Table *table = currentDatabase->getTable(tableName);

        std::string exec = SEL_PRE + tableName;
        Comm::send(&exec, 0);
        Comm::send(&exec, 1);
        std::vector<Record> rawRecords;
        if (table) {
            int64_t c;
            Comm::recv(&c, 0);
            auto count = (uint64_t) c;

            for (int i = 0; i < count; i++) {
                Record temp(table);
                for (int j = 0; j < table->getFieldTypes().size(); j++) {
                    int type = table->getFieldTypes()[j];
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
            for (const auto &field: table->getFieldNames()) {
                std::cout << std::setw(10) << field << " ";
            }
            std::cout << std::endl;
            for (const auto &record: rawRecords) {
                record.print();
            }

            Comm::recv(&done, 0);
            Comm::recv(&done, 1);
        } else {
            log("Table " + tableName + " does not exist.", false);
            throw std::runtime_error("");
        }

    } else if (word == "insert") {
        if (!currentDatabase) {
            log("Select a database first.", false);
            throw std::runtime_error("");
        }
        iss >> word;
        if (word != "into") {
            log("Syntax error. Maybe you should use `insert into`.", false);
            throw std::runtime_error("");
        }
        std::string tableName;
        iss >> tableName;
        std::string exec = INS_PRE + tableName;
        Comm::send(&exec, 0);
        Comm::send(&exec, 1);

        Table *table = currentDatabase->getTable(tableName);
        if (!table) {
            log("Table " + tableName + " does not exist.", false);
            throw std::runtime_error("");
        }

        iss >> word;
        if (word != "values") {
            log("Syntax error. Maybe you should use `insert into <table_name> values`.", false);
            throw std::runtime_error("");
        }

        std::string valuesStr;
        std::getline(iss, valuesStr, ')');
        valuesStr.erase(0, valuesStr.find('(') + 1);

        std::istringstream valuesStream(valuesStr);
        std::vector<int64_t> values;
        std::string value;

        int vi = 0;
        while (std::getline(valuesStream, value, ',')) {
            if (vi >= table->getFieldTypes().size()) {
                log("Unmatched parameter numbers.", false);
                throw std::runtime_error("");
            }
            value.erase(value.find_last_not_of(" \n\r\t") + 1);
            value.erase(0, value.find_first_not_of(" \n\r\t"));

            try {
                int64_t valueInt = std::stoll(value);
                int type = table->getFieldTypes()[vi];
                int64_t masked = valueInt & ((1l << type) - 1);
                if (type < 64 && masked != valueInt) {
                    throw std::out_of_range("");
                }
                values.emplace_back(valueInt);
            } catch (const std::out_of_range &) {
                log("Inserted parameters out of range.", false);
                throw std::runtime_error("");
            } catch (const std::invalid_argument &) {
                log("Invalid parameters.", false);
                throw std::runtime_error("");
            }
            vi++;
        }
        if (vi != table->getFieldTypes().size()) {
            log("Unmatched parameter numbers.", false);
            throw std::runtime_error("");
        }

        for (int i = 0; i < table->getFieldTypes().size(); i++) {
            int type = table->getFieldTypes()[i];
            auto v = values[i];
            if (type == 1) {
                BitSecret(v).share();
            } else if (type == 8) {
                IntSecret<int8_t>((int8_t) v).share();
            } else if (type == 16) {
                IntSecret<int16_t>((int16_t) v).share();
            } else if (type == 32) {
                IntSecret<int32_t>((int32_t) v).share();
            } else {
                IntSecret<int64_t>(v).share();
            }
        }

        log("Record inserted into " + tableName + ".", true);
        Comm::recv(&done, 0);
        Comm::recv(&done, 1);
    } else {
        log("Unknown command: " + command, false);
    }
}

void DBMS::handleConsole() {
    std::string command;

    std::cout << "Welcome to SMPC-DBMS. Type 'exit' to quit." << std::endl;

    while (true) {
        if (currentDatabase) {
            std::cout << "smpc-db(" + currentDatabase->getName() + ")> ";
        } else {
            std::cout << "smpc-db> ";
        }
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        std::getline(std::cin, command);
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        if (command == "exit") {
            Comm::send(&command, 0);
            Comm::send(&command, 1);
            break;
        }
        executeSQL(command);
    }

    std::cout << "Goodbye!" << std::endl;
}

int DBMS::parseDataType(const std::string &typeName) {
    if (typeName == "bool") {
        return 1;
    }
    if (typeName == "int8") {
        return 8;
    }
    if (typeName == "int16") {
        return 16;
    }
    if (typeName == "int32") {
        return 32;
    }
    if (typeName == "int64") {
        return 64;
    }
    throw std::invalid_argument("Unknown data type");
}

void DBMS::log(const std::string &msg, bool success) {
    Log::i((success ? "Done. " : "Failed. ") + msg);
}

void DBMS::handleRequests() {
    int done;
    while (true) {
        std::string command;
        Comm::recv(&command, Comm::CLIENT_RANK);

        if (command == "exit") {
            break;
        } else if (command.starts_with(CDB_PRE)) { // create database
            std::string dbName = command.substr(CDB_PRE.length());
            if (!createDatabase(dbName)) {
                throw std::runtime_error("");
            }
            Comm::send(&done, Comm::CLIENT_RANK);
        } else if (command.starts_with(DDB_PRE)) {
            std::string dbName = command.substr(DDB_PRE.length());
            if (!deleteDatabase(dbName)) {
                throw std::runtime_error("");
            }
            Comm::send(&done, Comm::CLIENT_RANK);
        } else if (command.starts_with(USE_PRE)) {
            std::string dbName = command.substr(USE_PRE.length());
            if (!useDatabase(dbName)) {
                throw std::runtime_error("");
            }

            Comm::send(&done, Comm::CLIENT_RANK);
        } else if (command.starts_with(CTB_PRE)) {
            std::istringstream iss(command.substr(CTB_PRE.length()));
            createTableByCommand(iss);
            Comm::send(&done, Comm::CLIENT_RANK);
        } else if (command.starts_with(DTB_PRE)) {
            std::string tableName = command.substr(DTB_PRE.length());
            std::string msg;
            if (!currentDatabase->deleteTable(tableName, msg)) {
                log(msg, false);
                throw std::runtime_error("");
            }
            Comm::send(&done, Comm::CLIENT_RANK);
        } else if (command.starts_with(INS_PRE)) {
            std::string tableName = command.substr(INS_PRE.length());
            Table *table = currentDatabase->getTable(tableName);
            if (!table) {
                log("Table does not exist.", false);
                throw std::runtime_error("");
            }

            std::vector<int> types = table->getFieldTypes();
            Record r(table);
            for (int i = 0; i < types.size(); i++) {
                int type = types[i];
                if (type == 1) {
                    BitSecret s = BitSecret(false).share();
                    r.addField(s, type);
                } else if (type == 8) {
                    IntSecret s = IntSecret<int8_t>(0).share();
                    r.addField(s, type);
                } else if (type == 16) {
                    IntSecret s = IntSecret<int16_t>(0).share();
                    r.addField(s, type);
                } else if (type == 32) {
                    IntSecret s = IntSecret<int32_t>(0).share();
                    r.addField(s, type);
                } else {
                    IntSecret s = IntSecret<int64_t>(0).share();
                    r.addField(s, type);
                }
            }
            if (table->insertRecord(r)) {
                log("Record inserted into " + tableName + ".", true);
            } else {
                log("Failed to insert record into " + tableName + ".", false);
                throw std::runtime_error("");
            }
            Comm::send(&done, Comm::CLIENT_RANK);
        } else if (command.starts_with(SEL_PRE)) {
            std::string tableName = command.substr(INS_PRE.length());

            Table *table = currentDatabase->getTable(tableName);
            std::vector<Record> records = table->getAllRecords();
            auto count = (int64_t) (records.size());
            if (Comm::rank() == 0) {
                Comm::send(&count, Comm::CLIENT_RANK);
            }

            for (int i = 0; i < count; i++) {
                const Record &cur = records[i];

                for (int j = 0; j < table->getFieldTypes().size(); j++) {
                    int type = table->getFieldTypes()[j];
                    if (type == 1) {
                        std::get<BitSecret>(cur.getValues()[j]).reconstruct();
                    } else if (type == 8) {
                        std::get<IntSecret<int8_t>>(cur.getValues()[j]).reconstruct();
                    } else if (type == 16) {
                        std::get<IntSecret<int16_t>>(cur.getValues()[j]).reconstruct();
                    } else if (type == 32) {
                        std::get<IntSecret<int32_t>>(cur.getValues()[j]).reconstruct();
                    } else {
                        std::get<IntSecret<int64_t>>(cur.getValues()[j]).reconstruct();
                    }
                }
            }
            Comm::send(&done, Comm::CLIENT_RANK);
        }
    }
}

void DBMS::handleFile() {
    std::string command;

    std::cout << "Welcome to SMPC-DBMS. Type 'exit' to quit." << std::endl;

    std::fstream file("commands.txt");

    if (!file.is_open()) {
        throw std::runtime_error("Cannot open commands.txt.");
    }

    while (std::getline(file, command)) {
        if (currentDatabase) {
            std::cout << "smpc-db(" + currentDatabase->getName() + ")> ";
        } else {
            std::cout << "smpc-db> ";
        }
        std::cout << command << std::endl;
        if (command == "exit") {
            Comm::send(&command, 0);
            Comm::send(&command, 1);
            break;
        }
        executeSQL(command);
    }

    std::cout << "Goodbye!" << std::endl;
}

void DBMS::createTableByCommand(std::istringstream &iss) {
    std::string tableName;
    iss >> tableName;

    if (currentDatabase->getTable(tableName)) {
        log("Table already exists.", false);
        return;
    }

    std::string fieldsStr;
    std::getline(iss, fieldsStr, ')');
    fieldsStr.erase(0, fieldsStr.find('(') + 1);
    std::istringstream fieldsStream(fieldsStr);

    std::vector<std::string> fieldNames;
    std::vector<int> fieldTypes;
    std::string fieldDef;
    while (std::getline(fieldsStream, fieldDef, ',')) {
        std::istringstream fieldStream(fieldDef);
        std::string fieldName, typeStr;
        fieldStream >> fieldName >> typeStr;

        try {
            int type = parseDataType(typeStr);
            fieldNames.push_back(fieldName);
            fieldTypes.push_back(type);
        } catch (const std::invalid_argument &e) {
            log("Invalid data type for field " + fieldName, false);
            return;
        }
    }

    std::string msg;
    bool created = currentDatabase->createTable(tableName, fieldNames, fieldTypes, msg);
    if (!created) {
        log(msg, false);
        throw std::runtime_error("");
    } else {
        log("Table " + tableName + " created.", true);
    }
}
