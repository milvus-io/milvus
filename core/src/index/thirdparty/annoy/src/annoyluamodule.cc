// Copyright (c) 2016 Boris Nagaev
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include <cstring>
#include <typeinfo>

#include <lua.hpp>

#include "annoylib.h"
#include "kissrandom.h"

#if LUA_VERSION_NUM == 501
#define compat_setfuncs(L, funcs) luaL_register(L, NULL, funcs)
#define compat_rawlen lua_objlen
#else
#define compat_setfuncs(L, funcs) luaL_setfuncs(L, funcs, 0)
#define compat_rawlen lua_rawlen
#endif

template<typename Distance>
class LuaAnnoy {
public:
  typedef int32_t AnnoyS;
  typedef float AnnoyT;
  typedef AnnoyIndex<AnnoyS, AnnoyT, Distance, Kiss64Random> Impl;
  typedef LuaAnnoy<Distance> ThisClass;

  class LuaArrayProxy {
  public:
    LuaArrayProxy(lua_State* L, int object, int f)
      : L_(L)
      , object_(object)
    {
      luaL_checktype(L, object, LUA_TTABLE);
      int v_len = compat_rawlen(L, object);
      luaL_argcheck(L, v_len == f, object, "Length of v != f");
    }

    double operator[](int index) const {
      lua_rawgeti(L_, object_, index + 1);
      double result = lua_tonumber(L_, -1);
      lua_pop(L_, 1);
      return result;
    }

  private:
    lua_State* L_;
    int object_;
  };

  static void toVector(lua_State* L, int object, int f, AnnoyT* dst) {
    LuaArrayProxy proxy(L, object, f);
    for (int i = 0; i < f; i++) {
      dst[i] = proxy[i];
    }
  }

  template <typename Vector>
  static void pushVector(lua_State* L, const Vector& v) {
    lua_createtable(L, v.size(), 0);
    for (int j = 0; j < v.size(); j++) {
      lua_pushnumber(L, v[j]);
      lua_rawseti(L, -2, j + 1);
    }
  }

  static const char* typeAsString() {
    return typeid(Impl).name();
  }

  static Impl* getAnnoy(lua_State* L, int object) {
    return reinterpret_cast<Impl*>(
      luaL_checkudata(L, object, typeAsString())
    );
  }

  static int getItemIndex(lua_State* L, int object, int size = -1) {
    int item = luaL_checkinteger(L, object);
    luaL_argcheck(L, item >= 0, object, "Index must be >= 0");
    if (size != -1) {
      luaL_argcheck(L, item < size, object, "Index must be < size");
    }
    return item;
  }

  static int gc(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    self->~Impl();
    return 0;
  }

  static int tostring(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    lua_pushfstring(
      L,
      "annoy.AnnoyIndex object (%dx%d, %s distance)",
      self->get_n_items(), self->get_f(), Distance::name()
    );
    return 1;
  }

  static int add_item(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    int item = getItemIndex(L, 2);
    self->add_item_impl(item, LuaArrayProxy(L, 3, self->get_f()));
    return 0;
  }

  static int build(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    int n_trees = luaL_checkinteger(L, 2);
    self->build(n_trees);
    lua_pushboolean(L, true);
    return 1;
  }

  static int on_disk_build(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    const char* filename = luaL_checkstring(L, 2);
    self->on_disk_build(filename);
    lua_pushboolean(L, true);
    return 1;
  }

  static int save(lua_State* L) {
    int nargs = lua_gettop(L);
    Impl* self = getAnnoy(L, 1);
    const char* filename = luaL_checkstring(L, 2);
    bool prefault = true;
    if (nargs >= 3) {
      prefault = lua_toboolean(L, 3);
    }
    self->save(filename, prefault);
    lua_pushboolean(L, true);
    return 1;
  }

  static int load(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    int nargs = lua_gettop(L);
    const char* filename = luaL_checkstring(L, 2);
    bool prefault = true;
    if (nargs >= 3) {
      prefault = lua_toboolean(L, 3);
    }
    if (!self->load(filename, prefault)) {
      return luaL_error(L, "Can't load file: %s", filename);
    }
    lua_pushboolean(L, true);
    return 1;
  }

  static int unload(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    self->unload();
    lua_pushboolean(L, true);
    return 1;
  }

  struct Searcher {
    std::vector<AnnoyS> result;
    std::vector<AnnoyT> distances;
    Impl* self;
    int n;
    int search_k;
    bool include_distances;

    Searcher(lua_State* L) {
      int nargs = lua_gettop(L);
      self = getAnnoy(L, 1);
      n = luaL_checkinteger(L, 3);
      search_k = -1;
      if (nargs >= 4) {
        search_k = luaL_checkinteger(L, 4);
      }
      include_distances = false;
      if (nargs >= 5) {
        include_distances = lua_toboolean(L, 5);
      }
    }

    int pushResults(lua_State* L) {
      pushVector(L, result);
      if (include_distances) {
        pushVector(L, distances);
      }
      return include_distances ? 2 : 1;
    }
  };

  static int get_nns_by_item(lua_State* L) {
    Searcher s(L);
    int item = getItemIndex(L, 2, s.self->get_n_items());
    s.self->get_nns_by_item(item, s.n, s.search_k, &s.result,
        s.include_distances ? &s.distances : NULL);
    return s.pushResults(L);
  }

  static int get_nns_by_vector(lua_State* L) {
    Searcher s(L);
    std::vector<AnnoyT> _vec(s.self->get_f());
    AnnoyT* vec = &(_vec[0]);
    toVector(L, 2, s.self->get_f(), vec);
    s.self->get_nns_by_vector(vec, s.n, s.search_k, &s.result,
        s.include_distances ? &s.distances : NULL);
    return s.pushResults(L);
  }

  static int get_item_vector(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    int item = getItemIndex(L, 2, self->get_n_items());
    std::vector<AnnoyT> _vec(self->get_f());
    AnnoyT* vec = &(_vec[0]);
    self->get_item(item, vec);
    pushVector(L, _vec);
    return 1;
  }

  static int get_distance(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    int i = getItemIndex(L, 2, self->get_n_items());
    int j = getItemIndex(L, 3, self->get_n_items());
    AnnoyT distance = self->get_distance(i, j);
    lua_pushnumber(L, distance);
    return 1;
  }

  static int get_n_items(lua_State* L) {
    Impl* self = getAnnoy(L, 1);
    lua_pushnumber(L, self->get_n_items());
    return 1;
  }

  static const luaL_Reg* getMetatable() {
    static const luaL_Reg funcs[] = {
      {"__gc", &ThisClass::gc},
      {"__tostring", &ThisClass::tostring},
      {NULL, NULL},
    };
    return funcs;
  }

  static const luaL_Reg* getMethods() {
    static const luaL_Reg funcs[] = {
      {"add_item", &ThisClass::add_item},
      {"build", &ThisClass::build},
      {"save", &ThisClass::save},
      {"load", &ThisClass::load},
      {"unload", &ThisClass::unload},
      {"get_nns_by_item", &ThisClass::get_nns_by_item},
      {"get_nns_by_vector", &ThisClass::get_nns_by_vector},
      {"get_item_vector", &ThisClass::get_item_vector},
      {"get_distance", &ThisClass::get_distance},
      {"get_n_items", &ThisClass::get_n_items},
      {"on_disk_build", &ThisClass::on_disk_build},
      {NULL, NULL},
    };
    return funcs;
  }

  static void createNew(lua_State* L, int f) {
    void* self = lua_newuserdata(L, sizeof(Impl));
    if (luaL_newmetatable(L, typeAsString())) {
      compat_setfuncs(L, getMetatable());
      lua_newtable(L);
      compat_setfuncs(L, getMethods());
      lua_setfield(L, -2, "__index");
    }
    new (self) Impl(f);
    lua_setmetatable(L, -2);
  }
};

static int lua_an_make(lua_State* L) {
  int f = luaL_checkinteger(L, 1);
  const char* metric = "angular";
  if (lua_gettop(L) >= 2) {
      metric = luaL_checkstring(L, 2);
  }
  if (strcmp(metric, "angular") == 0) {
    LuaAnnoy<Angular>::createNew(L, f);
    return 1;
  } else if (strcmp(metric, "euclidean") == 0) {
    LuaAnnoy<Euclidean>::createNew(L, f);
    return 1;
  } else if (strcmp(metric, "manhattan") == 0) {
    LuaAnnoy<Manhattan>::createNew(L, f);
    return 1;
  } else {
    return luaL_error(L, "Unknown metric: %s", metric);
  }
}

static const luaL_Reg LUA_ANNOY_FUNCS[] = {
  {"AnnoyIndex", lua_an_make},
  {NULL, NULL},
};

extern "C" {
int luaopen_annoy(lua_State* L) {
  lua_newtable(L);
  compat_setfuncs(L, LUA_ANNOY_FUNCS);
  return 1;
}
}

// vim: tabstop=2 shiftwidth=2
