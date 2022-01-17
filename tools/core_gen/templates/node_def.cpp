@@@@body@struct_name
void
@@struct_name@@::accept(@@root_base@@Visitor& visitor) {
    visitor.visit(*this);
}
####

@@@@main
// Generated File
// DO NOT EDIT
#include "query/@@root_base@@.h"
#include "@@root_base@@Visitor.h"

namespace @@namespace@@ {
@@body@@

}  // namespace @@namespace@@

####