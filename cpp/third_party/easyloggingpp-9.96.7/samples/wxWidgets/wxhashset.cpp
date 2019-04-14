#include <wx/hashset.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

WX_DECLARE_HASH_SET( int, wxIntegerHash, wxIntegerEqual, IntHashSet );
WX_DECLARE_HASH_SET( wxString, wxStringHash, wxStringEqual, StringHashSet );

ELPP_WX_ENABLED(IntHashSet)
ELPP_WX_ENABLED(StringHashSet)

int main() {

    IntHashSet h1;
    StringHashSet hStr;

    hStr.insert( "foo" );
    hStr.insert( "bar" );
    hStr.insert( "baz" );
    hStr.insert( "bar" ); // Does not add anything!

    LOG(INFO) << hStr;
}
