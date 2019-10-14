// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/VectorIndex.h"
#include "inc/Core/Common/DataUtils.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/SimpleIniReader.h"

#include "inc/Core/BKT/Index.h"
#include "inc/Core/KDT/Index.h"
#include <fstream>


using namespace SPTAG;


VectorIndex::VectorIndex()
{
}


VectorIndex::~VectorIndex()
{
}


std::string 
VectorIndex::GetParameter(const std::string& p_param) const
{
    return GetParameter(p_param.c_str());
}


ErrorCode
VectorIndex::SetParameter(const std::string& p_param, const std::string& p_value)
{
    return SetParameter(p_param.c_str(), p_value.c_str());
}


void 
VectorIndex::SetMetadata(const std::string& p_metadataFilePath, const std::string& p_metadataIndexPath) {
    m_pMetadata.reset(new FileMetadataSet(p_metadataFilePath, p_metadataIndexPath));
}


ByteArray 
VectorIndex::GetMetadata(IndexType p_vectorID) const {
    if (nullptr != m_pMetadata)
    {
        return m_pMetadata->GetMetadata(p_vectorID);
    }
    return ByteArray::c_empty;
}


ErrorCode 
VectorIndex::LoadIndex(const std::string& p_folderPath)
{
    std::string folderPath(p_folderPath);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep)
    {
        folderPath += FolderSep;
    }

    Helper::IniReader p_configReader;
    if (ErrorCode::Success != p_configReader.LoadIniFile(folderPath + "/indexloader.ini"))
    {
        return ErrorCode::FailedOpenFile;
    }

    std::string metadataSection("MetaData");
    if (p_configReader.DoesSectionExist(metadataSection))
    {
        std::string metadataFilePath = p_configReader.GetParameter(metadataSection,
            "MetaDataFilePath",
            std::string());
        std::string metadataIndexFilePath = p_configReader.GetParameter(metadataSection,
            "MetaDataIndexPath",
            std::string());

        m_pMetadata.reset(new FileMetadataSet(folderPath + metadataFilePath, folderPath + metadataIndexFilePath));

        if (!m_pMetadata->Available())
        {
            std::cerr << "Error: Failed to load metadata." << std::endl;
            return ErrorCode::Fail;
        }
    }
    if (DistCalcMethod::Undefined == p_configReader.GetParameter("Index", "DistCalcMethod", DistCalcMethod::Undefined))
    {
        std::cerr << "Error: Failed to load parameter DistCalcMethod." << std::endl;
        return ErrorCode::Fail;
    }

    return LoadIndex(folderPath, p_configReader);
}


ErrorCode VectorIndex::SaveIndex(const std::string& p_folderPath)
{
    std::string folderPath(p_folderPath);
    if (!folderPath.empty() && *(folderPath.rbegin()) != FolderSep)
    {
        folderPath += FolderSep;
    }

    if (!direxists(folderPath.c_str()))
    {
        mkdir(folderPath.c_str());
    }

    std::string loaderFilePath = folderPath + "indexloader.ini";

    std::ofstream loaderFile(loaderFilePath);
    if (!loaderFile.is_open())
    {
        return ErrorCode::FailedCreateFile;
    }

    if (nullptr != m_pMetadata)
    {
        std::string metadataFile = "metadata.bin";
        std::string metadataIndexFile = "metadataIndex.bin";
        loaderFile << "[MetaData]" << std::endl;
        loaderFile << "MetaDataFilePath=" << metadataFile << std::endl;
        loaderFile << "MetaDataIndexPath=" << metadataIndexFile << std::endl;
        loaderFile << std::endl;

        m_pMetadata->SaveMetadata(folderPath + metadataFile, folderPath + metadataIndexFile);
    }

    loaderFile << "[Index]" << std::endl;
    loaderFile << "IndexAlgoType=" << Helper::Convert::ConvertToString(GetIndexAlgoType()) << std::endl;
    loaderFile << "ValueType=" << Helper::Convert::ConvertToString(GetVectorValueType()) << std::endl;
    loaderFile << std::endl;

    ErrorCode ret = SaveIndex(folderPath, loaderFile);
    loaderFile.close();
    return ret;
}

ErrorCode
VectorIndex::BuildIndex(std::shared_ptr<VectorSet> p_vectorSet,
    std::shared_ptr<MetadataSet> p_metadataSet)
{
    if (nullptr == p_vectorSet || p_vectorSet->Count() == 0 || p_vectorSet->Dimension() == 0 || p_vectorSet->GetValueType() != GetVectorValueType())
    {
        return ErrorCode::Fail;
    }

    BuildIndex(p_vectorSet->GetData(), p_vectorSet->Count(), p_vectorSet->Dimension());
    m_pMetadata = std::move(p_metadataSet);
    return ErrorCode::Success;
}


ErrorCode
VectorIndex::SearchIndex(const void* p_vector, int p_neighborCount, std::vector<BasicResult>& p_results) const {
    QueryResult res(p_vector, p_neighborCount, p_results);
    SearchIndex(res);
    return ErrorCode::Success;
}


ErrorCode 
VectorIndex::AddIndex(std::shared_ptr<VectorSet> p_vectorSet, std::shared_ptr<MetadataSet> p_metadataSet) {
    if (nullptr == p_vectorSet || p_vectorSet->Count() == 0 || p_vectorSet->Dimension() == 0 || p_vectorSet->GetValueType() != GetVectorValueType())
    {
        return ErrorCode::Fail;
    }
    AddIndex(p_vectorSet->GetData(), p_vectorSet->Count(), p_vectorSet->Dimension());
    if (m_pMetadata == nullptr) {
        m_pMetadata = std::move(p_metadataSet);
    }
    else {
        m_pMetadata->AddBatch(*p_metadataSet);
    }
    return ErrorCode::Success;
}


std::shared_ptr<VectorIndex>
VectorIndex::CreateInstance(IndexAlgoType p_algo, VectorValueType p_valuetype)
{
    if (IndexAlgoType::Undefined == p_algo || VectorValueType::Undefined == p_valuetype)
    {
        return nullptr;
    }

    if (p_algo == IndexAlgoType::BKT) {
        switch (p_valuetype)
        {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name: \
        return std::shared_ptr<VectorIndex>(new BKT::Index<Type>); \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default: break;
        }
    }
    else if (p_algo == IndexAlgoType::KDT) {
        switch (p_valuetype)
        {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name: \
        return std::shared_ptr<VectorIndex>(new KDT::Index<Type>); \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default: break;
        }
    }
    return nullptr;
}


ErrorCode
VectorIndex::LoadIndex(const std::string& p_loaderFilePath, std::shared_ptr<VectorIndex>& p_vectorIndex)
{
    Helper::IniReader iniReader;

    if (ErrorCode::Success != iniReader.LoadIniFile(p_loaderFilePath + "/indexloader.ini"))
    {
        return ErrorCode::FailedOpenFile;
    }

    IndexAlgoType algoType = iniReader.GetParameter("Index", "IndexAlgoType", IndexAlgoType::Undefined);
    VectorValueType valueType = iniReader.GetParameter("Index", "ValueType", VectorValueType::Undefined);
    if (IndexAlgoType::Undefined == algoType || VectorValueType::Undefined == valueType)
    {
        return ErrorCode::Fail;
    }

    if (algoType == IndexAlgoType::BKT) {
        switch (valueType)
        {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name: \
        p_vectorIndex.reset(new BKT::Index<Type>); \
        p_vectorIndex->LoadIndex(p_loaderFilePath); \
        break; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default: break;
        }
    }
    else if (algoType == IndexAlgoType::KDT) {
        switch (valueType)
        {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name: \
        p_vectorIndex.reset(new KDT::Index<Type>); \
        p_vectorIndex->LoadIndex(p_loaderFilePath); \
        break; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default: break;
        }
    }
    return ErrorCode::Success;
}


ErrorCode VectorIndex::MergeIndex(const char* p_indexFilePath1, const char* p_indexFilePath2)
{
    std::string folderPath1(p_indexFilePath1), folderPath2(p_indexFilePath2);
    if (!folderPath1.empty() && *(folderPath1.rbegin()) != FolderSep) folderPath1 += FolderSep;
    if (!folderPath2.empty() && *(folderPath2.rbegin()) != FolderSep) folderPath2 += FolderSep;

    Helper::IniReader p_configReader1, p_configReader2;
    if (ErrorCode::Success != p_configReader1.LoadIniFile(folderPath1 + "/indexloader.ini"))
        return ErrorCode::FailedOpenFile;

    if (ErrorCode::Success != p_configReader2.LoadIniFile(folderPath2 + "/indexloader.ini"))
        return ErrorCode::FailedOpenFile;

    std::shared_ptr<VectorIndex> index = CreateInstance(
        p_configReader1.GetParameter("Index", "IndexAlgoType", IndexAlgoType::Undefined), 
        p_configReader1.GetParameter("Index", "ValueType", VectorValueType::Undefined));
    if (index == nullptr) return ErrorCode::FailedParseValue;

    std::string empty("");
    if (!COMMON::DataUtils::MergeIndex(folderPath1 + p_configReader1.GetParameter("Index", "VectorFilePath", empty),
        folderPath1 + p_configReader1.GetParameter("MetaData", "MetaDataFilePath", empty),
        folderPath1 + p_configReader1.GetParameter("MetaData", "MetaDataIndexPath", empty),
        folderPath2 + p_configReader1.GetParameter("Index", "VectorFilePath", empty),
        folderPath2 + p_configReader1.GetParameter("MetaData", "MetaDataFilePath", empty),
        folderPath2 + p_configReader1.GetParameter("MetaData", "MetaDataIndexPath", empty)))
        return ErrorCode::Fail;

    for (const auto& iter : p_configReader1.GetParameters("Index"))
        index->SetParameter(iter.first.c_str(), iter.second.c_str());

    if (p_configReader1.DoesSectionExist("MetaData"))
    {
        for (const auto& iter : p_configReader1.GetParameters("MetaData"))
            index->SetParameter(iter.first.c_str(), iter.second.c_str());
        index->SetMetadata(folderPath1 + p_configReader1.GetParameter("MetaData", "MetaDataFilePath", empty),
                           folderPath1 + p_configReader1.GetParameter("MetaData", "MetaDataIndexPath", empty));
    }

    std::ifstream vecIn(folderPath1 + p_configReader1.GetParameter("Index", "VectorFilePath", empty), std::ios::binary);
    int R, C;
    vecIn.read((char*)&R, sizeof(int));
    vecIn.read((char*)&C, sizeof(int));
    size_t size = R * C * GetValueTypeSize(index->GetVectorValueType());
    char* data = new char[size];
    vecIn.read(data, size);
    vecIn.close();
    index->BuildIndex((void*)data, R, C);
    index->SaveIndex(folderPath1);
    return ErrorCode::Success;
}