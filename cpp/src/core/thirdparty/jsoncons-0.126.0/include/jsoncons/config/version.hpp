// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_VERSION_HPP
#define JSONCONS_VERSION_HPP

#include <iostream>
    
#define JSONCONS_VERSION_MAJOR 0
#define JSONCONS_VERSION_MINOR 125
#define JSONCONS_VERSION_PATCH 0

namespace jsoncons {

struct versioning_info
{
    unsigned int const major;
    unsigned int const minor;
    unsigned int const patch;

    friend std::ostream& operator<<(std::ostream& os, const versioning_info& ver)
    {
        os << ver.major << '.'
           << ver.minor << '.'
           << ver.patch;
        return os;
    }
}; 

constexpr versioning_info version()
{
    return versioning_info{JSONCONS_VERSION_MAJOR, JSONCONS_VERSION_MINOR, JSONCONS_VERSION_PATCH};
}

}

#endif
