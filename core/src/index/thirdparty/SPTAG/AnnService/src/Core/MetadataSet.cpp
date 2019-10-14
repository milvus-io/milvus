// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/MetadataSet.h"

#include <fstream>
#include <iostream>
#include <cstring>

using namespace SPTAG;

ErrorCode
MetadataSet::RefineMetadata(std::vector<int>& indices, const std::string& p_folderPath)
{
    std::ofstream metaOut(p_folderPath + "metadata.bin_tmp", std::ios::binary);
    std::ofstream metaIndexOut(p_folderPath + "metadataIndex.bin", std::ios::binary);
    if (!metaOut.is_open() || !metaIndexOut.is_open()) return ErrorCode::FailedCreateFile;

    int R = (int)indices.size();
    metaIndexOut.write((char*)&R, sizeof(int));
    std::uint64_t offset = 0;
    for (int i = 0; i < R; i++) {
        metaIndexOut.write((char*)&offset, sizeof(std::uint64_t));
        ByteArray meta = GetMetadata(indices[i]);
        metaOut.write((char*)meta.Data(), sizeof(uint8_t)*meta.Length());
        offset += meta.Length();
    }
    metaOut.close();
    metaIndexOut.write((char*)&offset, sizeof(std::uint64_t));
    metaIndexOut.close();

    SPTAG::MetadataSet::MetaCopy(p_folderPath + "metadata.bin_tmp", p_folderPath + "metadata.bin");
    return ErrorCode::Success;
}


ErrorCode
MetadataSet::MetaCopy(const std::string& p_src, const std::string& p_dst)
{
    if (p_src == p_dst) return ErrorCode::Success;

    std::ifstream src(p_src, std::ios::binary);
    if (!src.is_open())
    {
        std::cerr << "ERROR: Can't open " << p_src << std::endl;
        return ErrorCode::FailedOpenFile;
    }

    std::ofstream dst(p_dst, std::ios::binary);
    if (!dst.is_open())
    {
        std::cerr << "ERROR: Can't create " << p_dst << std::endl;
        src.close();
        return ErrorCode::FailedCreateFile;
    }

    int bufsize = 1000000;
    char* buf = new char[bufsize];
    while (!src.eof()) {
        src.read(buf, bufsize);
        dst.write(buf, src.gcount());
    }
    delete[] buf;
    src.close();
    dst.close();

    return ErrorCode::Success;
}

MetadataSet::MetadataSet()
{
}


MetadataSet:: ~MetadataSet()
{
}


FileMetadataSet::FileMetadataSet(const std::string& p_metafile, const std::string& p_metaindexfile)
    : m_metaFile(p_metafile),
      m_metaindexFile(p_metaindexfile)
{
    m_fp = new std::ifstream(p_metafile, std::ifstream::binary);
    std::ifstream fpidx(p_metaindexfile, std::ifstream::binary);
    if (!m_fp->is_open() || !fpidx.is_open())
    {
        std::cerr << "ERROR: Cannot open meta files " << p_metafile << " and " << p_metaindexfile << "!" << std::endl;
        return;
    }

    fpidx.read((char *)&m_count, sizeof(m_count));
    m_pOffsets.resize(m_count + 1);
    fpidx.read((char *)m_pOffsets.data(), sizeof(std::uint64_t) * (m_count + 1));
    fpidx.close();
}


FileMetadataSet::~FileMetadataSet()
{
    if (m_fp)
    {
        m_fp->close();
        delete m_fp;
    }
}


ByteArray
FileMetadataSet::GetMetadata(IndexType p_vectorID) const
{
    std::uint64_t startoff = m_pOffsets[p_vectorID];
    std::uint64_t bytes = m_pOffsets[p_vectorID + 1] - startoff;
    if (p_vectorID < (IndexType)m_count) {
        m_fp->seekg(startoff, std::ios_base::beg);
        ByteArray b = ByteArray::Alloc((SizeType)bytes);
        m_fp->read((char*)b.Data(), bytes);
        return b;
    }
    else {
        startoff -= m_pOffsets[m_count];
        return ByteArray((std::uint8_t*)m_newdata.data() + startoff, static_cast<SizeType>(bytes), false);
    }
}


SizeType
FileMetadataSet::Count() const
{
    return static_cast<SizeType>(m_pOffsets.size() - 1);
}


bool
FileMetadataSet::Available() const
{
    return m_fp && m_fp->is_open() && m_pOffsets.size() > 1;
}


void
FileMetadataSet::AddBatch(MetadataSet& data)
{
    for (int i = 0; i < static_cast<int>(data.Count()); i++)
    {
        ByteArray newdata = data.GetMetadata(i);
        m_newdata.insert(m_newdata.end(), newdata.Data(), newdata.Data() + newdata.Length());
        m_pOffsets.push_back(m_pOffsets[m_pOffsets.size() - 1] + newdata.Length());
    }
}


ErrorCode
FileMetadataSet::SaveMetadata(const std::string& p_metaFile, const std::string& p_metaindexFile)
{
    ErrorCode ret = ErrorCode::Success;
    m_fp->close();
    ret = MetaCopy(m_metaFile, p_metaFile);
    if (ErrorCode::Success != ret)
    {
        return ret;
    }
    if (m_newdata.size() > 0) {
        std::ofstream tmpout(p_metaFile, std::ofstream::app|std::ios::binary);
        if (!tmpout.is_open()) return ErrorCode::FailedOpenFile;
        tmpout.write((char*)m_newdata.data(), m_newdata.size());
        tmpout.close();
    }
    m_fp->open(p_metaFile, std::ifstream::binary);

    std::ofstream dst(p_metaindexFile, std::ios::binary);
    m_count = static_cast<int>(m_pOffsets.size()) - 1;
    m_newdata.clear();
    dst.write((char*)&m_count, sizeof(m_count));
    dst.write((char*)m_pOffsets.data(), sizeof(std::uint64_t) * m_pOffsets.size());
    return ret;
}


ErrorCode
FileMetadataSet::SaveMetadataToMemory(void **pGraphMemFile, int64_t &len) {
    // TODO(lxj): serialize file to mem?
    return ErrorCode::Fail;
}

ErrorCode
FileMetadataSet::LoadMetadataFromMemory(void *pGraphMemFile) {
    // TODO(lxj): not support yet
    return ErrorCode::Fail;
}

MemMetadataSet::MemMetadataSet(ByteArray p_metadata, ByteArray p_offsets, SizeType p_count)
    : m_metadataHolder(std::move(p_metadata)),
      m_offsetHolder(std::move(p_offsets)),
      m_count(p_count)
{
    const std::uint64_t* newdata = reinterpret_cast<const std::uint64_t*>(m_offsetHolder.Data());
    m_offsets.insert(m_offsets.end(), newdata, newdata + p_count + 1);
}


MemMetadataSet::~MemMetadataSet()
{
}


ByteArray
MemMetadataSet::GetMetadata(IndexType p_vectorID) const
{
    if (static_cast<SizeType>(p_vectorID) < m_count)
    {
        return ByteArray(m_metadataHolder.Data() + m_offsets[p_vectorID],
                         static_cast<SizeType>(m_offsets[p_vectorID + 1] - m_offsets[p_vectorID]),
                         m_metadataHolder.DataHolder());
    }
    else if (p_vectorID < m_offsets.size() - 1) {
        return ByteArray((std::uint8_t*)m_newdata.data() + m_offsets[p_vectorID] - m_offsets[m_count],
            static_cast<SizeType>(m_offsets[p_vectorID + 1] - m_offsets[p_vectorID]),
            false);
    }

    return ByteArray::c_empty;
}


SizeType
MemMetadataSet::Count() const
{
    return m_count;
}


bool
MemMetadataSet::Available() const
{
    return m_metadataHolder.Length() > 0 && m_offsetHolder.Length() > 0;
}

void
MemMetadataSet::AddBatch(MetadataSet& data)
{
    for (int i = 0; i < static_cast<int>(data.Count()); i++)
    {
        ByteArray newdata = data.GetMetadata(i);
        m_newdata.insert(m_newdata.end(), newdata.Data(), newdata.Data() + newdata.Length());
        m_offsets.push_back(m_offsets[m_offsets.size() - 1] + newdata.Length());
    }
}

ErrorCode
MemMetadataSet::SaveMetadata(const std::string& p_metaFile, const std::string& p_metaindexFile)
{
    std::ofstream outputStream;
    outputStream.open(p_metaFile, std::ios::binary);
    if (!outputStream.is_open())
    {
        std::cerr << "Error: Failed to create file " << p_metaFile << "." << std::endl;
        return ErrorCode::FailedCreateFile;
    }

    outputStream.write(reinterpret_cast<const char*>(m_metadataHolder.Data()), m_metadataHolder.Length());
    outputStream.write((const char*)m_newdata.data(), sizeof(std::uint8_t)*m_newdata.size());
    outputStream.close();

    outputStream.open(p_metaindexFile, std::ios::binary);
    if (!outputStream.is_open())
    {
        std::cerr << "Error: Failed to create file " << p_metaindexFile << "." << std::endl;
        return ErrorCode::FailedCreateFile;
    }

    m_count = static_cast<int>(m_offsets.size()) - 1;
    outputStream.write(reinterpret_cast<const char*>(&m_count), sizeof(m_count));
    outputStream.write(reinterpret_cast<const char*>(m_offsets.data()), sizeof(std::uint64_t)*m_offsets.size());
    outputStream.close();

    return ErrorCode::Success;
}

ErrorCode
MemMetadataSet::SaveMetadataToMemory(void **pGraphMemFile, int64_t &len) {
    auto size = sizeof(int64_t) + sizeof(int64_t) + m_metadataHolder.Length() + sizeof(std::uint64_t) * m_offsets.size();
    char* mem = (char*)malloc(size);
    if (mem == NULL) return ErrorCode::Fail;

    auto ptr = mem;
    *(int64_t*)ptr = m_metadataHolder.Length();
    ptr += sizeof(int64_t);

    m_count = static_cast<int>(m_offsets.size()) - 1;
    *(int64_t*)ptr = m_count;
    ptr += sizeof(int64_t);

    memcpy(ptr, m_metadataHolder.Data(), m_metadataHolder.Length());
    ptr += m_metadataHolder.Length();

    memcpy(ptr, m_offsets.data(), sizeof(std::uint64_t)*m_offsets.size());

    *pGraphMemFile = mem;
    len = size;

    return ErrorCode::Success;
}

ErrorCode
MemMetadataSet::LoadMetadataFromMemory(void *pGraphMemFile) {
    m_metadataHolder.Clear();
    m_offsetHolder.Clear();
    m_offsets.clear();

    char* ptr = (char *)pGraphMemFile;
    auto metadataHolderLength = *(int64_t *)ptr;
    ptr += sizeof(int64_t);

    m_count = *(int64_t *)ptr;
    ptr += sizeof(int64_t);

    m_metadataHolder = ByteArray::Alloc(metadataHolderLength);
    memcpy(m_metadataHolder.Data(), ptr, metadataHolderLength);
    ptr += metadataHolderLength;

    m_offsetHolder = ByteArray::Alloc(sizeof(std::uint64_t ) * (m_count + 1));
    memcpy(m_offsetHolder.Data(), ptr, sizeof(std::uint64_t ) * (m_count + 1));

    const std::uint64_t* newdata = reinterpret_cast<const std::uint64_t*>(m_offsetHolder.Data());
    m_offsets.insert(m_offsets.end(), newdata, newdata + m_count + 1);

    return ErrorCode::Success;
}
