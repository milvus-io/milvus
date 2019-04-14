#include <wx/string.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main() {
    wxString str = "This is a simple wxString";
    LOG(INFO) << str;
}
