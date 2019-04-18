////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "db_connection.h"


namespace zilliz {
namespace vecwise {
namespace engine {

using std::string;
using namespace sqlite_orm;

string storage_file_name = "default.sqlite";

SqliteDBPtr connect() {
    SqliteDBPtr temp = std::make_shared<SqliteDB>(initStorage(storage_file_name));
    temp->sync_schema();
    temp->open_forever(); // thread safe option
    //temp->pragma.journal_mode(journal_mode::WAL); // WAL => write ahead log
    return temp;
}

SqliteDBPtr Connection::connect_ = connect();

}
}
}
