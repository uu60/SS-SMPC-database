#include <sstream>
#include "mpc_package/utils/Comm.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;
#include "socket/LocalServer.h"
#include "dbms/DBMS.h"

int main(int argc, char **argv) {
    Comm::init(argc, argv);

    if (Comm::rank() == Comm::CLIENT_RANK) {
        LocalServer &server = LocalServer::getInstance();
        server.run();
    } else {
        DBMS::getInstance().handleRequests();
    }

    Comm::finalize();
    return 0;

}
