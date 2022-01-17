// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_INDEXBUILDER_OPTIONS_H_
#define _SPTAG_INDEXBUILDER_OPTIONS_H_

#include "inc/Core/Common.h"
#include "inc/Helper/VectorSetReader.h"

#include <string>
#include <vector>
#include <memory>

namespace SPTAG
{
namespace IndexBuilder
{

class BuilderOptions : public Helper::ReaderOptions
{
public:
    BuilderOptions();

    ~BuilderOptions();

    std::string m_inputFiles;

    std::string m_outputFolder;

    SPTAG::IndexAlgoType m_indexAlgoType;

    std::string m_builderConfigFile;
};


} // namespace IndexBuilder
} // namespace SPTAG

#endif // _SPTAG_INDEXBUILDER_OPTIONS_H_
