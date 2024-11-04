//
// Created by 杜建璋 on 2024/11/3.
//

#ifndef SELECT_H
#define SELECT_H
#include <sstream>
#include <hsql/SQLParser.h>

#include "dbms/SystemManager.h"

class Select {
public:
    static bool clientSelect(std::ostringstream &resp, const hsql::SQLStatement *stmt);

    static void serverSelect(json j);
};


#endif //SELECT_H
