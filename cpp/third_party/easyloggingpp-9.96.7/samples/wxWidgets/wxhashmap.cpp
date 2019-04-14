#include <wx/hashmap.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

WX_DECLARE_STRING_HASH_MAP( wxString, MyHashMap);

ELPP_WX_HASH_MAP_ENABLED(MyHashMap)

int main() {

    MyHashMap h1;
    h1["Batman"] = "Joker";
    h1["Spiderman"] = "Venom";

    LOG(INFO) << h1;
}
