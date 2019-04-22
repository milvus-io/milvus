//  Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifdef LUA

// lua headers
extern "C" {
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
}

namespace rocksdb {
namespace lua {
// A class that used to define custom C Library that is callable
// from Lua script
class RocksLuaCustomLibrary {
 public:
  virtual ~RocksLuaCustomLibrary() {}
  // The name of the C library.  This name will also be used as the table
  // (namespace) in Lua that contains the C library.
  virtual const char* Name() const = 0;

  // Returns a "static const struct luaL_Reg[]", which includes a list of
  // C functions.  Note that the last entry of this static array must be
  // {nullptr, nullptr} as required by Lua.
  //
  // More details about how to implement Lua C libraries can be found
  // in the official Lua document http://www.lua.org/pil/26.2.html
  virtual const struct luaL_Reg* Lib() const = 0;

  // A function that will be called right after the library has been created
  // and pushed on the top of the lua_State.  This custom setup function
  // allows developers to put additional table or constant values inside
  // the same table / namespace.
  virtual void CustomSetup(lua_State* /*L*/) const {}
};
}  // namespace lua
}  // namespace rocksdb
#endif  // LUA
