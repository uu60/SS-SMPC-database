//
// Created by 杜建璋 on 2024/11/3.
//

#ifndef INSERT_H
#define INSERT_H
#include <sstream>
#include <nlohmann/json.hpp>
#include <sql/SQLStatement.h>


class Insert {
public:
    static bool clientInsert(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverInsert(nlohmann::basic_json<> j);
};



#endif //INSERT_H
