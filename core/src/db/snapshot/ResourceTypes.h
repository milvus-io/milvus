#pragma once

#include <string>
#include <vector>
#include <set>

using ID_TYPE = int64_t;
using NUM_TYPE = int64_t;
using FTYPE_TYPE = int64_t;
using TS_TYPE = int64_t;
using MappingT = std::set<ID_TYPE>;

using IDS_TYPE = std::vector<ID_TYPE>;

enum State {
    PENDING = 0,
    ACTIVE = 1,
    DEACTIVE = 2
};
