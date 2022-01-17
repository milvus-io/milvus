@@@@base_visitor
@@root_base@@Visitor
####
@@@@body@struct_name
    void
    visit(@@struct_name@@& @@parameter_name@@) override;
####
@@@@main
#pragma once
// Generated File
// DO NOT EDIT
@@extra_inc@@
#include "@@base_visitor@@.h"

namespace @@namespace@@ {
class @@visitor_name@@ : public @@base_visitor@@ {
 public:
@@body@@

@@ctor_and_member@@
};
}  // namespace @@namespace@@

####
