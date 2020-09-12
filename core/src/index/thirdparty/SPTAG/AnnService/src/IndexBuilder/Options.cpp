// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/IndexBuilder/Options.h"
#include "inc/Helper/StringConvert.h"

#include <cassert>

using namespace SPTAG;
using namespace SPTAG::IndexBuilder;


BuilderOptions::BuilderOptions()
    : Helper::ReaderOptions(VectorValueType::Float, 0, "|", 32)
{
    AddRequiredOption(m_inputFiles, "-i", "--input", "Input raw data.");
    AddRequiredOption(m_outputFolder, "-o", "--outputfolder", "Output folder.");
    AddRequiredOption(m_indexAlgoType, "-a", "--algo", "Index Algorithm type.");
    AddOptionalOption(m_builderConfigFile, "-c", "--config", "Config file for builder.");
}


BuilderOptions::~BuilderOptions()
{
}
