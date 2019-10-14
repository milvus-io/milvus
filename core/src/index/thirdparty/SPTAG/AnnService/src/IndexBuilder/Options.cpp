// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/IndexBuilder/Options.h"
#include "inc/Helper/StringConvert.h"

#include <cassert>

using namespace SPTAG;
using namespace SPTAG::IndexBuilder;


BuilderOptions::BuilderOptions()
    : m_threadNum(32),
      m_inputValueType(VectorValueType::Float),
      m_vectorDelimiter("|")
{
    AddOptionalOption(m_threadNum, "-t", "--thread", "Thread Number.");
    AddOptionalOption(m_vectorDelimiter, "", "--delimiter", "Vector delimiter.");
    AddRequiredOption(m_dimension, "-d", "--dimension", "Dimension of vector.");
    AddRequiredOption(m_inputValueType, "-v", "--vectortype", "Input vector data type. Default is float.");
    AddRequiredOption(m_inputFiles, "-i", "--input", "Input raw data.");
    AddRequiredOption(m_outputFolder, "-o", "--outputfolder", "Output folder.");
    AddRequiredOption(m_indexAlgoType, "-a", "--algo", "Index Algorithm type.");
    AddOptionalOption(m_builderConfigFile, "-c", "--config", "Config file for builder.");
}


BuilderOptions::~BuilderOptions()
{
}
