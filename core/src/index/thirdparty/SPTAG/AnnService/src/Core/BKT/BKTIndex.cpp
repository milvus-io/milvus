// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/BKT/Index.h"

#pragma warning(disable:4996)  // 'fopen': This function or variable may be unsafe. Consider using fopen_s instead. To disable deprecation, use _CRT_SECURE_NO_WARNINGS. See online help for details.
#pragma warning(disable:4242)  // '=' : conversion from 'int' to 'short', possible loss of data
#pragma warning(disable:4244)  // '=' : conversion from 'int' to 'short', possible loss of data
#pragma warning(disable:4127)  // conditional expression is constant

namespace SPTAG
{
    namespace BKT
    {
        template <typename T>
        ErrorCode Index<T>::LoadConfig(Helper::IniReader& p_reader)
        {
#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
            SetParameter(RepresentStr, \
                         p_reader.GetParameter("Index", \
                         RepresentStr, \
                         std::string(#DefaultValue)).c_str()); \

#include "inc/Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::LoadIndexDataFromMemory(const std::vector<ByteArray>& p_indexBlobs)
        {
            if (p_indexBlobs.size() < 3) return ErrorCode::LackOfInputs;

            if (!m_pSamples.Load((char*)p_indexBlobs[0].Data())) return ErrorCode::FailedParseValue;
            if (!m_pTrees.LoadTrees((char*)p_indexBlobs[1].Data())) return ErrorCode::FailedParseValue;
            if (!m_pGraph.LoadGraph((char*)p_indexBlobs[2].Data())) return ErrorCode::FailedParseValue;
            if (p_indexBlobs.size() > 3 && !m_deletedID.load((char*)p_indexBlobs[3].Data())) return ErrorCode::FailedParseValue;

            m_workSpacePool.reset(new COMMON::WorkSpacePool(m_iMaxCheck, GetNumSamples()));
            m_workSpacePool->Init(m_iNumberOfThreads);
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::LoadIndexData(const std::string& p_folderPath)
        {
            if (!m_pSamples.Load(p_folderPath + m_sDataPointsFilename)) return ErrorCode::Fail;
            if (!m_pTrees.LoadTrees(p_folderPath + m_sBKTFilename)) return ErrorCode::Fail;
            if (!m_pGraph.LoadGraph(p_folderPath + m_sGraphFilename)) return ErrorCode::Fail;
            if (!m_deletedID.load(p_folderPath + m_sDeleteDataPointsFilename)) return ErrorCode::Fail;

            m_workSpacePool.reset(new COMMON::WorkSpacePool(m_iMaxCheck, GetNumSamples()));
            m_workSpacePool->Init(m_iNumberOfThreads);
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::SaveConfig(std::ostream& p_configOut) const
        {
#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    p_configOut << RepresentStr << "=" << GetParameter(RepresentStr) << std::endl;

#include "inc/Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter
            p_configOut << std::endl;
            return ErrorCode::Success;
        }

        template<typename T>
        ErrorCode
            Index<T>::SaveIndexData(const std::string& p_folderPath)
        {
            std::lock_guard<std::mutex> lock(m_dataAddLock);
            std::shared_lock<std::shared_timed_mutex> sharedlock(m_deletedID.getLock());

            if (!m_pSamples.Save(p_folderPath + m_sDataPointsFilename)) return ErrorCode::Fail;
            if (!m_pTrees.SaveTrees(p_folderPath + m_sBKTFilename)) return ErrorCode::Fail;
            if (!m_pGraph.SaveGraph(p_folderPath + m_sGraphFilename)) return ErrorCode::Fail;
            if (!m_deletedID.save(p_folderPath + m_sDeleteDataPointsFilename)) return ErrorCode::Fail;
            return ErrorCode::Success;
        }

        template<typename T>
        ErrorCode Index<T>::SaveIndexData(const std::vector<std::ostream*>& p_indexStreams)
        {
            if (p_indexStreams.size() < 4) return ErrorCode::LackOfInputs;
            
            std::lock_guard<std::mutex> lock(m_dataAddLock);
            std::shared_lock<std::shared_timed_mutex> sharedlock(m_deletedID.getLock());

            if (!m_pSamples.Save(*p_indexStreams[0])) return ErrorCode::Fail;
            if (!m_pTrees.SaveTrees(*p_indexStreams[1])) return ErrorCode::Fail;
            if (!m_pGraph.SaveGraph(*p_indexStreams[2])) return ErrorCode::Fail;
            if (!m_deletedID.save(*p_indexStreams[3])) return ErrorCode::Fail;
            return ErrorCode::Success;
        }

#pragma region K-NN search

#define Search(CheckDeleted1) \
        m_pTrees.InitSearchTrees(this, p_query, p_space); \
        const DimensionType checkPos = m_pGraph.m_iNeighborhoodSize - 1; \
        while (!p_space.m_SPTQueue.empty()) { \
            m_pTrees.SearchTrees(this, p_query, p_space, m_iNumberOfOtherDynamicPivots + p_space.m_iNumberOfCheckedLeaves); \
            while (!p_space.m_NGQueue.empty()) { \
                COMMON::HeapCell gnode = p_space.m_NGQueue.pop(); \
                const SizeType *node = m_pGraph[gnode.node]; \
                _mm_prefetch((const char *)node, _MM_HINT_T0); \
                CheckDeleted1 { \
                    if (p_query.AddPoint(gnode.node, gnode.distance)) { \
                        p_space.m_iNumOfContinuousNoBetterPropagation = 0; \
                        SizeType checkNode = node[checkPos]; \
                        if (checkNode < -1) { \
                            const COMMON::BKTNode& tnode = m_pTrees[-2 - checkNode]; \
                            for (SizeType i = -tnode.childStart; i < tnode.childEnd; i++) { \
                                if (!p_query.AddPoint(m_pTrees[i].centerid, gnode.distance)) break; \
                            } \
                        } \
                    } \
                    else { \
                        p_space.m_iNumOfContinuousNoBetterPropagation++; \
                        if (p_space.m_iNumOfContinuousNoBetterPropagation > p_space.m_iContinuousLimit || p_space.m_iNumberOfCheckedLeaves > p_space.m_iMaxCheck) { \
                            p_query.SortResult(); return; \
                        } \
                    } \
                } \
                for (DimensionType i = 0; i <= checkPos; i++) { \
                    _mm_prefetch((const char *)(m_pSamples)[node[i]], _MM_HINT_T0); \
                } \
                for (DimensionType i = 0; i <= checkPos; i++) { \
                    SizeType nn_index = node[i]; \
                    if (nn_index < 0) break; \
                    if (p_space.CheckAndSet(nn_index)) continue; \
                    float distance2leaf = m_fComputeDistance(p_query.GetTarget(), (m_pSamples)[nn_index], GetFeatureDim()); \
                    p_space.m_iNumberOfCheckedLeaves++; \
                    p_space.m_NGQueue.insert(COMMON::HeapCell(nn_index, distance2leaf)); \
                } \
                if (p_space.m_NGQueue.Top().distance > p_space.m_SPTQueue.Top().distance) { \
                    break; \
                } \
            } \
        } \
        p_query.SortResult(); \

        template <typename T>
        void Index<T>::SearchIndexWithDeleted(COMMON::QueryResultSet<T> &p_query, COMMON::WorkSpace &p_space, const Helper::Concurrent::ConcurrentSet<SizeType> &p_deleted) const
        {
            Search(if (!p_deleted.contains(gnode.node)))
        }

        template <typename T>
        void Index<T>::SearchIndexWithoutDeleted(COMMON::QueryResultSet<T> &p_query, COMMON::WorkSpace &p_space) const
        {
            Search(;)
        }

        template<typename T>
        ErrorCode
            Index<T>::SearchIndex(QueryResult &p_query) const
        {
            auto workSpace = m_workSpacePool->Rent();
            workSpace->Reset(m_iMaxCheck);

            if (m_deletedID.size() > 0)
                SearchIndexWithDeleted(*((COMMON::QueryResultSet<T>*)&p_query), *workSpace, m_deletedID);
            else
                SearchIndexWithoutDeleted(*((COMMON::QueryResultSet<T>*)&p_query), *workSpace);

            m_workSpacePool->Return(workSpace);

            if (p_query.WithMeta() && nullptr != m_pMetadata)
            {
                for (int i = 0; i < p_query.GetResultNum(); ++i)
                {
                    SizeType result = p_query.GetResult(i)->VID;
                    p_query.SetMetadata(i, (result < 0) ? ByteArray::c_empty : m_pMetadata->GetMetadata(result));
                }
            }
            return ErrorCode::Success;
        }
#pragma endregion

        template <typename T>
        ErrorCode Index<T>::BuildIndex(const void* p_data, SizeType p_vectorNum, DimensionType p_dimension)
        {
            omp_set_num_threads(m_iNumberOfThreads);

            m_pSamples.Initialize(p_vectorNum, p_dimension, (T*)p_data, false);

            if (DistCalcMethod::Cosine == m_iDistCalcMethod)
            {
                int base = COMMON::Utils::GetBase<T>();
#pragma omp parallel for
                for (SizeType i = 0; i < GetNumSamples(); i++) {
                    COMMON::Utils::Normalize(m_pSamples[i], GetFeatureDim(), base);
                }
            }

            m_workSpacePool.reset(new COMMON::WorkSpacePool(m_iMaxCheck, GetNumSamples()));
            m_workSpacePool->Init(m_iNumberOfThreads);
            
            m_pTrees.BuildTrees<T>(this);
            m_pGraph.BuildGraph<T>(this, &(m_pTrees.GetSampleMap()));

            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::RefineIndex(const std::vector<std::ostream*>& p_indexStreams)
        {
            std::lock_guard<std::mutex> lock(m_dataAddLock);
            std::shared_lock<std::shared_timed_mutex> sharedlock(m_deletedID.getLock());

            SizeType newR = GetNumSamples();

            std::vector<SizeType> indices;
            std::vector<SizeType> reverseIndices(newR);
            for (SizeType i = 0; i < newR; i++) {
                if (!m_deletedID.contains(i)) {
                    indices.push_back(i);
                    reverseIndices[i] = i;
                }
                else {
                    while (m_deletedID.contains(newR - 1) && newR > i) newR--;
                    if (newR == i) break;
                    indices.push_back(newR - 1);
                    reverseIndices[newR - 1] = i;
                    newR--;
                }
            }

            std::cout << "Refine... from " << GetNumSamples() << "->" << newR << std::endl;

            if (false == m_pSamples.Refine(indices, *p_indexStreams[0])) return ErrorCode::Fail;
            if (nullptr != m_pMetadata && (p_indexStreams.size() < 6 || ErrorCode::Success != m_pMetadata->RefineMetadata(indices, *p_indexStreams[4], *p_indexStreams[5]))) return ErrorCode::Fail;

            COMMON::BKTree newTrees(m_pTrees);
            newTrees.BuildTrees<T>(this, &indices);
#pragma omp parallel for
            for (SizeType i = 0; i < newTrees.size(); i++) {
                newTrees[i].centerid = reverseIndices[newTrees[i].centerid];
            }
            newTrees.SaveTrees(*p_indexStreams[1]);

            m_pGraph.RefineGraph<T>(this, indices, reverseIndices, *p_indexStreams[2], &(newTrees.GetSampleMap()));

            Helper::Concurrent::ConcurrentSet<SizeType> newDeletedID;
            newDeletedID.save(*p_indexStreams[3]);
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::RefineIndex(const std::string& p_folderPath)
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

            std::vector<std::ostream*> streams;
            streams.push_back(new std::ofstream(folderPath + m_sDataPointsFilename, std::ios::binary));
            streams.push_back(new std::ofstream(folderPath + m_sBKTFilename, std::ios::binary));
            streams.push_back(new std::ofstream(folderPath + m_sGraphFilename, std::ios::binary));
            streams.push_back(new std::ofstream(folderPath + m_sDeleteDataPointsFilename, std::ios::binary));
            if (nullptr != m_pMetadata)
            {
                streams.push_back(new std::ofstream(folderPath + m_sMetadataFile, std::ios::binary));
                streams.push_back(new std::ofstream(folderPath + m_sMetadataIndexFile, std::ios::binary));
            }

            for (size_t i = 0; i < streams.size(); i++)
                if (!(((std::ofstream*)streams[i])->is_open())) return ErrorCode::FailedCreateFile;

            ErrorCode ret = RefineIndex(streams);

            for (size_t i = 0; i < streams.size(); i++)
            {
                ((std::ofstream*)streams[i])->close();
                delete streams[i];
            }
            return ret;
        }

        template <typename T>
        ErrorCode Index<T>::DeleteIndex(const void* p_vectors, SizeType p_vectorNum) {
            const T* ptr_v = (const T*)p_vectors;
#pragma omp parallel for schedule(dynamic)
            for (SizeType i = 0; i < p_vectorNum; i++) {
                COMMON::QueryResultSet<T> query(ptr_v + i * GetFeatureDim(), m_pGraph.m_iCEF);
                SearchIndex(query);

                for (int i = 0; i < m_pGraph.m_iCEF; i++) {
                    if (query.GetResult(i)->Dist < 1e-6) {
                        m_deletedID.insert(query.GetResult(i)->VID);
                    }
                }
            }
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::DeleteIndex(const SizeType& p_id) {
            m_deletedID.insert(p_id);
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode Index<T>::AddIndex(const void* p_vectors, SizeType p_vectorNum, DimensionType p_dimension, SizeType* p_start)
        {
            SizeType begin, end;
            {
                std::lock_guard<std::mutex> lock(m_dataAddLock);

                begin = GetNumSamples();
                end = GetNumSamples() + p_vectorNum;

                if (p_start != nullptr) *p_start = begin;
                
                if (begin == 0) return BuildIndex(p_vectors, p_vectorNum, p_dimension);

                if (p_dimension != GetFeatureDim()) return ErrorCode::FailedParseValue;

                if (m_pSamples.AddBatch((const T*)p_vectors, p_vectorNum) != ErrorCode::Success || m_pGraph.AddBatch(p_vectorNum) != ErrorCode::Success) {
                    std::cout << "Memory Error: Cannot alloc space for vectors" << std::endl;
                    m_pSamples.SetR(begin);
                    m_pGraph.SetR(begin);
                    return ErrorCode::MemoryOverFlow;
                }
                if (DistCalcMethod::Cosine == m_iDistCalcMethod)
                {
                    int base = COMMON::Utils::GetBase<T>();
                    for (SizeType i = begin; i < end; i++) {
                        COMMON::Utils::Normalize((T*)m_pSamples[i], GetFeatureDim(), base);
                    }
                }
            }

            for (SizeType node = begin; node < end; node++)
            {
                m_pGraph.RefineNode<T>(this, node, true);
            }
            std::cout << "Add " << p_vectorNum << " vectors" << std::endl;
            return ErrorCode::Success;
        }

        template <typename T>
        ErrorCode
            Index<T>::SetParameter(const char* p_param, const char* p_value)
        {
            if (nullptr == p_param || nullptr == p_value) return ErrorCode::Fail;

#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    else if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) \
    { \
        fprintf(stderr, "Setting %s with value %s\n", RepresentStr, p_value); \
        VarType tmp; \
        if (SPTAG::Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) \
        { \
            VarName = tmp; \
        } \
    } \

#include "inc/Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

            m_fComputeDistance = COMMON::DistanceCalcSelector<T>(m_iDistCalcMethod);
            return ErrorCode::Success;
        }


        template <typename T>
        std::string
            Index<T>::GetParameter(const char* p_param) const
        {
            if (nullptr == p_param) return std::string();

#define DefineBKTParameter(VarName, VarType, DefaultValue, RepresentStr) \
    else if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) \
    { \
        return SPTAG::Helper::Convert::ConvertToString(VarName); \
    } \

#include "inc/Core/BKT/ParameterDefinitionList.h"
#undef DefineBKTParameter

            return std::string();
        }
    }
}

#define DefineVectorValueType(Name, Type) \
template class SPTAG::BKT::Index<Type>; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType


