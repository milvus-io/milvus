// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/ArgumentsParser.h"

using namespace SPTAG::Helper;


ArgumentsParser::IArgument::IArgument()
{
}


ArgumentsParser::IArgument::~IArgument()
{
}


ArgumentsParser::ArgumentsParser()
{
}


ArgumentsParser::~ArgumentsParser()
{
}


bool
ArgumentsParser::Parse(int p_argc, char** p_args)
{
    while (p_argc > 0)
    {
        int last = p_argc;
        for (auto& option : m_arguments)
        {
            if (!option->ParseValue(p_argc, p_args))
            {
                fprintf(stderr, "Failed to parse args around \"%s\"\n", *p_args);
                PrintHelp();
                return false;
            }
        }

        if (last == p_argc)
        {
            p_argc -= 1;
            p_args += 1;
        }
    }

    bool isValid = true;
    for (auto& option : m_arguments)
    {
        if (option->IsRequiredButNotSet())
        {
            fprintf(stderr, "Required option not set:\n  ");
            option->PrintDescription(stderr);
            fprintf(stderr, "\n");
            isValid = false;
        }
    }

    if (!isValid)
    {
        fprintf(stderr, "\n");
        PrintHelp();
        return false;
    }

    return true;
}


void
ArgumentsParser::PrintHelp()
{
    fprintf(stderr, "Usage: ");
    for (auto& option : m_arguments)
    {
        fprintf(stderr, "\n  ");
        option->PrintDescription(stderr);
    }

    fprintf(stderr, "\n\n");
}
