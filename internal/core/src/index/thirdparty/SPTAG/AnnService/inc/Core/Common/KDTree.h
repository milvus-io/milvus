// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_KDTREE_H_
#define _SPTAG_COMMON_KDTREE_H_

#include <iostream>
#include <vector>
#include <string>

#include "../VectorIndex.h"

#include "CommonUtils.h"
#include "QueryResultSet.h"
#include "WorkSpace.h"

#pragma warning(disable:4996)  // 'fopen': This function or variable may be unsafe. Consider using fopen_s instead. To disable deprecation, use _CRT_SECURE_NO_WARNINGS. See online help for details.

namespace SPTAG
{
    namespace COMMON
    {
        // node type for storing KDT
        struct KDTNode
        {
            SizeType left;
            SizeType right;
            DimensionType split_dim;
            float split_value;
        };

        class KDTree
        {
        public:
            KDTree() : m_iTreeNumber(2), m_numTopDimensionKDTSplit(5), m_iSamples(1000) {}

            KDTree(KDTree& other) : m_iTreeNumber(other.m_iTreeNumber),
                m_numTopDimensionKDTSplit(other.m_numTopDimensionKDTSplit),
                m_iSamples(other.m_iSamples) {}
            ~KDTree() {}

            inline const KDTNode& operator[](SizeType index) const { return m_pTreeRoots[index]; }
            inline KDTNode& operator[](SizeType index) { return m_pTreeRoots[index]; }

            inline SizeType size() const { return (SizeType)m_pTreeRoots.size(); }

            template <typename T>
            void BuildTrees(VectorIndex* p_index, std::vector<SizeType>* indices = nullptr)
            {
                std::vector<SizeType> localindices;
                if (indices == nullptr) {
                    localindices.resize(p_index->GetNumSamples());
                    for (SizeType i = 0; i < p_index->GetNumSamples(); i++) localindices[i] = i;
                }
                else {
                    localindices.assign(indices->begin(), indices->end());
                }

                m_pTreeRoots.resize(m_iTreeNumber * localindices.size());
                m_pTreeStart.resize(m_iTreeNumber, 0);
#pragma omp parallel for
                for (int i = 0; i < m_iTreeNumber; i++)
                {
                    Sleep(i * 100); std::srand(clock());

                    std::vector<SizeType> pindices(localindices.begin(), localindices.end());
                    std::random_shuffle(pindices.begin(), pindices.end());

                    m_pTreeStart[i] = i * (SizeType)pindices.size();
                    std::cout << "Start to build KDTree " << i + 1 << std::endl;
                    SizeType iTreeSize = m_pTreeStart[i];
                    DivideTree<T>(p_index, pindices, 0, (SizeType)pindices.size() - 1, m_pTreeStart[i], iTreeSize);
                    std::cout << i + 1 << " KDTree built, " << iTreeSize - m_pTreeStart[i] << " " << pindices.size() << std::endl;
                }
            }

            inline std::uint64_t BufferSize() const 
            { 
                return sizeof(int) + sizeof(SizeType) * m_iTreeNumber + 
                    sizeof(SizeType) + sizeof(KDTNode) * m_pTreeRoots.size();
            }

            bool SaveTrees(std::ostream& p_outstream) const
            {
                p_outstream.write((char*)&m_iTreeNumber, sizeof(int));
                p_outstream.write((char*)m_pTreeStart.data(), sizeof(SizeType) * m_iTreeNumber);
                SizeType treeNodeSize = (SizeType)m_pTreeRoots.size();
                p_outstream.write((char*)&treeNodeSize, sizeof(SizeType));
                p_outstream.write((char*)m_pTreeRoots.data(), sizeof(KDTNode) * treeNodeSize);
                std::cout << "Save KDT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            bool SaveTrees(std::string sTreeFileName) const
            {
                std::cout << "Save KDT to " << sTreeFileName << std::endl;
                std::ofstream output(sTreeFileName, std::ios::binary);
                if (!output.is_open()) return false;
                SaveTrees(output);
                output.close();
                return true;
            }

            bool LoadTrees(char*  pKDTMemFile)
            {
                m_iTreeNumber = *((int*)pKDTMemFile);
                pKDTMemFile += sizeof(int);
                m_pTreeStart.resize(m_iTreeNumber);
                memcpy(m_pTreeStart.data(), pKDTMemFile, sizeof(SizeType) * m_iTreeNumber);
                pKDTMemFile += sizeof(SizeType)*m_iTreeNumber;

                SizeType treeNodeSize = *((SizeType*)pKDTMemFile);
                pKDTMemFile += sizeof(SizeType);
                m_pTreeRoots.resize(treeNodeSize);
                memcpy(m_pTreeRoots.data(), pKDTMemFile, sizeof(KDTNode) * treeNodeSize);
                std::cout << "Load KDT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            bool LoadTrees(std::string sTreeFileName)
            {
                std::cout << "Load KDT From " << sTreeFileName << std::endl;
                std::ifstream input(sTreeFileName, std::ios::binary);
                if (!input.is_open()) return false;

                input.read((char*)&m_iTreeNumber, sizeof(int));
                m_pTreeStart.resize(m_iTreeNumber);
                input.read((char*)m_pTreeStart.data(), sizeof(SizeType) * m_iTreeNumber);

                SizeType treeNodeSize;
                input.read((char*)&treeNodeSize, sizeof(SizeType));
                m_pTreeRoots.resize(treeNodeSize);
                input.read((char*)m_pTreeRoots.data(), sizeof(KDTNode) * treeNodeSize);
                input.close();
                std::cout << "Load KDT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            template <typename T>
            void InitSearchTrees(const VectorIndex* p_index, const COMMON::QueryResultSet<T> &p_query, COMMON::WorkSpace &p_space,  const int p_limits) const
            {
                for (int i = 0; i < m_iTreeNumber; i++) {
                    KDTSearch(p_index, p_query, p_space, m_pTreeStart[i], true, 0);
                }

                while (!p_space.m_SPTQueue.empty() && p_space.m_iNumberOfCheckedLeaves < p_limits)
                {
                    auto& tcell = p_space.m_SPTQueue.pop();
                    if (p_query.worstDist() < tcell.distance) break;
                    KDTSearch(p_index, p_query, p_space, tcell.node, true, tcell.distance);
                }
            }

            template <typename T>
            void SearchTrees(const VectorIndex* p_index, const COMMON::QueryResultSet<T> &p_query, COMMON::WorkSpace &p_space, const int p_limits) const
            {
                while (!p_space.m_SPTQueue.empty() && p_space.m_iNumberOfCheckedLeaves < p_limits)
                {
                    auto& tcell = p_space.m_SPTQueue.pop();
                    KDTSearch(p_index, p_query, p_space, tcell.node, false, tcell.distance);
                }
            }

        private:

            template <typename T>
            void KDTSearch(const VectorIndex* p_index, const COMMON::QueryResultSet<T> &p_query,
                           COMMON::WorkSpace& p_space, const SizeType node, const bool isInit, const float distBound) const {
                if (node < 0)
                {
                    SizeType index = -node - 1;
                    if (index >= p_index->GetNumSamples()) return;
#ifdef PREFETCH
                    const char* data = (const char *)(p_index->GetSample(index));
                    _mm_prefetch(data, _MM_HINT_T0);
                    _mm_prefetch(data + 64, _MM_HINT_T0);
#endif
                    if (p_space.CheckAndSet(index)) return;

                    ++p_space.m_iNumberOfTreeCheckedLeaves;
                    ++p_space.m_iNumberOfCheckedLeaves;
                    p_space.m_NGQueue.insert(COMMON::HeapCell(index, p_index->ComputeDistance((const void*)p_query.GetTarget(), (const void*)data)));
                    return;
                }

                auto& tnode = m_pTreeRoots[node];

                float diff = (p_query.GetTarget())[tnode.split_dim] - tnode.split_value;
                float distanceBound = distBound + diff * diff;
                SizeType otherChild, bestChild;
                if (diff < 0)
                {
                    bestChild = tnode.left;
                    otherChild = tnode.right;
                }
                else
                {
                    otherChild = tnode.left;
                    bestChild = tnode.right;
                }

                if (!isInit || distanceBound < p_query.worstDist())
                {
                    p_space.m_SPTQueue.insert(COMMON::HeapCell(otherChild, distanceBound));
                }
                KDTSearch(p_index, p_query, p_space, bestChild, isInit, distBound);
            }


            template <typename T>
            void DivideTree(VectorIndex* p_index, std::vector<SizeType>& indices, SizeType first, SizeType last,
                SizeType index, SizeType &iTreeSize) {
                ChooseDivision<T>(p_index, m_pTreeRoots[index], indices, first, last);
                SizeType i = Subdivide<T>(p_index, m_pTreeRoots[index], indices, first, last);
                if (i - 1 <= first)
                {
                    m_pTreeRoots[index].left = -indices[first] - 1;
                }
                else
                {
                    iTreeSize++;
                    m_pTreeRoots[index].left = iTreeSize;
                    DivideTree<T>(p_index, indices, first, i - 1, iTreeSize, iTreeSize);
                }
                if (last == i)
                {
                    m_pTreeRoots[index].right = -indices[last] - 1;
                }
                else
                {
                    iTreeSize++;
                    m_pTreeRoots[index].right = iTreeSize;
                    DivideTree<T>(p_index, indices, i, last, iTreeSize, iTreeSize);
                }
            }

            template <typename T>
            void ChooseDivision(VectorIndex* p_index, KDTNode& node, const std::vector<SizeType>& indices, const SizeType first, const SizeType last)
            {
                std::vector<float> meanValues(p_index->GetFeatureDim(), 0);
                std::vector<float> varianceValues(p_index->GetFeatureDim(), 0);
                SizeType end = min(first + m_iSamples, last);
                SizeType count = end - first + 1;
                // calculate the mean of each dimension
                for (SizeType j = first; j <= end; j++)
                {
                    const T* v = (const T*)p_index->GetSample(indices[j]);
                    for (DimensionType k = 0; k < p_index->GetFeatureDim(); k++)
                    {
                        meanValues[k] += v[k];
                    }
                }
                for (DimensionType k = 0; k < p_index->GetFeatureDim(); k++)
                {
                    meanValues[k] /= count;
                }
                // calculate the variance of each dimension
                for (SizeType j = first; j <= end; j++)
                {
                    const T* v = (const T*)p_index->GetSample(indices[j]);
                    for (DimensionType k = 0; k < p_index->GetFeatureDim(); k++)
                    {
                        float dist = v[k] - meanValues[k];
                        varianceValues[k] += dist*dist;
                    }
                }
                // choose the split dimension as one of the dimension inside TOP_DIM maximum variance
                node.split_dim = SelectDivisionDimension(varianceValues);
                // determine the threshold
                node.split_value = meanValues[node.split_dim];
            }

            DimensionType SelectDivisionDimension(const std::vector<float>& varianceValues) const
            {
                // Record the top maximum variances
                std::vector<DimensionType> topind(m_numTopDimensionKDTSplit);
                int num = 0;
                // order the variances
                for (DimensionType i = 0; i < (DimensionType)varianceValues.size(); i++)
                {
                    if (num < m_numTopDimensionKDTSplit || varianceValues[i] > varianceValues[topind[num - 1]])
                    {
                        if (num < m_numTopDimensionKDTSplit)
                        {
                            topind[num++] = i;
                        }
                        else
                        {
                            topind[num - 1] = i;
                        }
                        int j = num - 1;
                        // order the TOP_DIM variances
                        while (j > 0 && varianceValues[topind[j]] > varianceValues[topind[j - 1]])
                        {
                            std::swap(topind[j], topind[j - 1]);
                            j--;
                        }
                    }
                }
                // randomly choose a dimension from TOP_DIM
                return topind[COMMON::Utils::rand(num)];
            }

            template <typename T>
            SizeType Subdivide(VectorIndex* p_index, const KDTNode& node, std::vector<SizeType>& indices, const SizeType first, const SizeType last) const
            {
                SizeType i = first;
                SizeType j = last;
                // decide which child one point belongs
                while (i <= j)
                {
                    SizeType ind = indices[i];
                    const T* v = (const T*)p_index->GetSample(ind);
                    float val = v[node.split_dim];
                    if (val < node.split_value)
                    {
                        i++;
                    }
                    else
                    {
                        std::swap(indices[i], indices[j]);
                        j--;
                    }
                }
                // if all the points in the node are equal,equally split the node into 2
                if ((i == first) || (i == last + 1))
                {
                    i = (first + last + 1) / 2;
                }
                return i;
            }

        private:
            std::vector<SizeType> m_pTreeStart;
            std::vector<KDTNode> m_pTreeRoots;

        public:
            int m_iTreeNumber, m_numTopDimensionKDTSplit, m_iSamples;
        };
    }
}
#endif
