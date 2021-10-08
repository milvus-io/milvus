// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/IndexBuilder/Options.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Core/Common.h"
#include "inc/Helper/SimpleIniReader.h"

#include <memory>
#include <iostream>

using namespace SPTAG;

int main(int argc, char* argv[])
{
    std::shared_ptr<IndexBuilder::BuilderOptions> options(new IndexBuilder::BuilderOptions);
    if (!options->Parse(argc - 1, argv + 1))
    {
        exit(1);
    }

    auto indexBuilder = VectorIndex::CreateInstance(options->m_indexAlgoType, options->m_inputValueType);

    Helper::IniReader iniReader;
    if (!options->m_builderConfigFile.empty())
    {
        iniReader.LoadIniFile(options->m_builderConfigFile);
    }

    for (int i = 1; i < argc; i++)
    {
        std::string param(argv[i]);
        size_t idx = param.find("=");
        if (idx == std::string::npos) continue;

        std::string paramName = param.substr(0, idx);
        std::string paramVal = param.substr(idx + 1);
        std::string sectionName;
        idx = paramName.find(".");
        if (idx != std::string::npos) {
            sectionName = paramName.substr(0, idx);
            paramName = paramName.substr(idx + 1);
        }
        iniReader.SetParameter(sectionName, paramName, paramVal);
        std::cout << "Set [" << sectionName << "]" << paramName << " = " << paramVal << std::endl;
    }

    if (!iniReader.DoesParameterExist("Index", "NumberOfThreads")) {
        iniReader.SetParameter("Index", "NumberOfThreads", std::to_string(options->m_threadNum));
    }
    for (const auto& iter : iniReader.GetParameters("Index"))
    {
        indexBuilder->SetParameter(iter.first.c_str(), iter.second.c_str());
    }

    ErrorCode code;
    if (options->m_inputFiles.find("BIN:") == 0) {
        std::vector<std::string> files = SPTAG::Helper::StrUtils::SplitString(options->m_inputFiles.substr(4), ",");
        std::ifstream inputStream(files[0], std::ifstream::binary);
        if (!inputStream.is_open()) {
            fprintf(stderr, "Failed to read input file.\n");
            exit(1);
        }
        SizeType row;
        DimensionType col;
        inputStream.read((char*)&row, sizeof(SizeType));
        inputStream.read((char*)&col, sizeof(DimensionType));
        std::uint64_t totalRecordVectorBytes = ((std::uint64_t)GetValueTypeSize(options->m_inputValueType)) * row * col;
        ByteArray vectorSet = ByteArray::Alloc(totalRecordVectorBytes);
        char* vecBuf = reinterpret_cast<char*>(vectorSet.Data());
        inputStream.read(vecBuf, totalRecordVectorBytes);
        inputStream.close();
        std::shared_ptr<VectorSet> p_vectorSet(new BasicVectorSet(vectorSet, options->m_inputValueType, col, row));
        
        std::shared_ptr<MetadataSet> p_metaSet = nullptr;
        if (files.size() >= 3) {
            p_metaSet.reset(new FileMetadataSet(files[1], files[2]));
        }
        code = indexBuilder->BuildIndex(p_vectorSet, p_metaSet);
        indexBuilder->SaveIndex(options->m_outputFolder);
    }
    else {
        auto vectorReader = Helper::VectorSetReader::CreateInstance(options);
        if (ErrorCode::Success != vectorReader->LoadFile(options->m_inputFiles))
        {
            fprintf(stderr, "Failed to read input file.\n");
            exit(1);
        }
        code = indexBuilder->BuildIndex(vectorReader->GetVectorSet(), vectorReader->GetMetadataSet());
        indexBuilder->SaveIndex(options->m_outputFolder);
    }

    if (ErrorCode::Success != code)
    {
        fprintf(stderr, "Failed to build index.\n");
        exit(1);
    }
    return 0;
}
