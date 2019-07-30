/// Copyright 2013-2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_SER_CONTEXT_HPP
#define JSONCONS_SER_CONTEXT_HPP

namespace jsoncons {

class ser_context
{
public:
    virtual ~ser_context() = default;

    virtual size_t line() const = 0;

    virtual size_t column() const = 0; 

#if !defined(JSONCONS_NO_DEPRECATED)
    size_t line_number() const
    {
        return line();
    }

    size_t column_number() const 
    {
        return column();
    }
#endif
};

class null_ser_context : public ser_context
{
private:
    size_t line() const override { return 0; }

    size_t column() const override { return 0; }
};

#if !defined(JSONCONS_NO_DEPRECATED)
typedef ser_context parsing_context;
typedef ser_context serializing_context;
typedef null_ser_context null_parsing_context;
typedef null_ser_context null_serializing_context;
#endif

}
#endif
