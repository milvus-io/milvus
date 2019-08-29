// Copyright 2019 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CBOR_CBOR_OPTIONS_HPP
#define JSONCONS_CBOR_CBOR_OPTIONS_HPP

#include <string>
#include <limits> // std::numeric_limits
#include <cwchar>
#include <jsoncons/json_exception.hpp>
#include <jsoncons_ext/cbor/cbor_detail.hpp>

namespace jsoncons { namespace cbor {

class cbor_decode_options
{
public:
    virtual ~cbor_decode_options() = default;
};

class cbor_encode_options
{
public:
    virtual ~cbor_encode_options() = default;

    virtual bool pack_strings() const = 0; 
};

class cbor_options : public virtual cbor_decode_options, 
                     public virtual cbor_encode_options
{
private:
    bool pack_strings_;
public:

    static const cbor_options& default_options()
    {
        static cbor_options options{};
        return options;
    }


//  Constructors

    cbor_options()
        : pack_strings_(false)
    {
    }

    bool pack_strings() const override
    {
        return pack_strings_;
    }

    cbor_options& pack_strings(bool value)
    {
        pack_strings_ = value;
        return *this;
    }
};

}}
#endif
