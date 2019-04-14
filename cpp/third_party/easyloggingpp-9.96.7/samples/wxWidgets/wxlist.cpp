#include <wx/list.h>
#include <wx/listimpl.cpp>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

WX_DECLARE_LIST(int, MyList);

WX_DEFINE_LIST(MyList);

// Following enables MyList to be log-friendly
ELPP_WX_PTR_ENABLED(MyList)

int main() {
    MyList list;
    for (int i = 1; i < 110; ++i)
        list.Append(new int (i));
    LOG(INFO) << list;
}
