@@@@body@struct_name
    virtual void
    visit(@@struct_name@@&) = 0;
####
@@@@main
#pragma once
// Generated File
// DO NOT EDIT
#include "query/@@root_base@@.h"
namespace @@namespace@@ {
class @@root_base@@Visitor {
 public:
    virtual ~@@root_base@@Visitor() = default;

 public:
@@body@@
};
}  // namespace @@namespace@@

####