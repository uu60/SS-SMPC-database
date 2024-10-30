#include <iostream>
#include <sstream>
#include "basis/Database.h"
#include "dbms/DBMS.h"
#include "mpc_package/utils/Comm.h"
#include "mpc_package/utils/Log.h"

int main(int argc, char **argv) {
    Comm::init(argc, argv);

    DBMS &dbms = DBMS::getInstance();
    if (Comm::isClient()) {
        dbms.handleFileCommands();
    } else {
        dbms.handleRequests();
    }

    Comm::finalize();
    return 0;
}
