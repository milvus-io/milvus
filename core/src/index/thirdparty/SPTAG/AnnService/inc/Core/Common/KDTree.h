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
            int left;
            int right;
            short split_dim;
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

            inline const KDTNode& operator[](int index) const { return m_pTreeRoots[index]; }
            inline KDTNode& operator[](int index) { return m_pTreeRoots[index]; }

            inline int size() const { return (int)m_pTreeRoots.size(); }

            template <typename T>
            void BuildTrees(VectorIndex* p_index, std::vector<int>* indices = nullptr)
            {
                std::vector<int> localindices;
                if (indices == nullptr) {
                    localindices.resize(p_index->GetNumSamples());
                    for (int i = 0; i < p_index->GetNumSamples(); i++) localindices[i] = i;
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

                    std::vector<int> pindices(localindices.begin(), localindices.end());
                    std::random_shuffle(pindices.begin(), pindices.end());

                    m_pTreeStart[i] = i * (int)pindices.size();
                    std::cout << "Start to build KDTree " << i + 1 << std::endl;
                    int iTreeSize = m_pTreeStart[i];
                    DivideTree<T>(p_index, pindices, 0, (int)pindices.size() - 1, m_pTreeStart[i], iTreeSize);
                    std::cout << i + 1 << " KDTree built, " << iTreeSize - m_pTreeStart[i] << " " << pindices.size() << std::endl;
                }
            }

            bool SaveTrees(void **pKDTMemFile, int64_t &len) const
            {
                int treeNodeSize = (int)m_pTreeRoots.size();

                size_t size = sizeof(int) +
                    sizeof(int) * m_iTreeNumber +
                    sizeof(int) +
                    sizeof(KDTNode) * treeNodeSize;
                char *mem = (char*)malloc(size);
                if (mem == NULL) return false;

                auto ptr = mem;
                *(int*)ptr = m_iTreeNumber;
                ptr += sizeof(int);

                memcpy(ptr, m_pTreeStart.data(), sizeof(int) * m_iTreeNumber);
                ptr += sizeof(int) * m_iTreeNumber;

                *(int*)ptr = treeNodeSize;
                ptr += sizeof(int);

                memcpy(ptr, m_pTreeRoots.data(), sizeof(KDTNode) * treeNodeSize);
                *pKDTMemFile = mem;
                len = size;

                return true;
            }

            bool SaveTrees(std::string sTreeFileName) const
            {
                std::cout << "Save KDT to " << sTreeFileName << std::endl;
                FILE *fp = fopen(sTreeFileName.c_str(), "wb");
                if (fp == NULL) return false;

                fwrite(&m_iTreeNumber, sizeof(int), 1, fp);
                fwrite(m_pTreeStart.data(), sizeof(int), m_iTreeNumber, fp);
                int treeNodeSize = (int)m_pTreeRoots.size();
                fwrite(&treeNodeSize, sizeof(int), 1, fp);
                fwrite(m_pTreeRoots.data(), sizeof(KDTNode), treeNodeSize, fp);
                fclose(fp);
                std::cout << "Save KDT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            bool LoadTrees(char*  pKDTMemFile)
            {
                m_iTreeNumber = *((int*)pKDTMemFile);
                pKDTMemFile += sizeof(int);
                m_pTreeStart.resize(m_iTreeNumber);
                memcpy(m_pTreeStart.data(), pKDTMemFile, sizeof(int) * m_iTreeNumber);
                pKDTMemFile += sizeof(int)*m_iTreeNumber;

                int treeNodeSize = *((int*)pKDTMemFile);
                pKDTMemFile += sizeof(int);
                m_pTreeRoots.resize(treeNodeSize);
                memcpy(m_pTreeRoots.data(), pKDTMemFile, sizeof(KDTNode) * treeNodeSize);
                return true;
            }

            bool LoadTrees(std::string sTreeFileName)
            {
                std::cout << "Load KDT From " << sTreeFileName << std::endl;
                FILE *fp = fopen(sTreeFileName.c_str(), "rb");
                if (fp == NULL) return false;

                fread(&m_iTreeNumber, sizeof(int), 1, fp);
                m_pTreeStart.resize(m_iTreeNumber);
                fread(m_pTreeStart.data(), sizeof(int), m_iTreeNumber, fp);

                int treeNodeSize;
                fread(&treeNodeSize, sizeof(int), 1, fp);
                m_pTreeRoots.resize(treeNodeSize);
                fread(m_pTreeRoots.data(), sizeof(KDTNode), treeNodeSize, fp);
                fclose(fp);
                std::cout << "Load KDT (" << m_iTreeNumber << "," << treeNodeSize << ") Finish!" << std::endl;
                return true;
            }

            template <typename T>
            void InitSearchTrees(const VectorIndex* p_index, const COMMON::QueryResultSet<T> &p_query, COMMON::WorkSpace &p_space,  const int p_limits) const
            {
                for (char i = 0; i < m_iTreeNumber; i++) {
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
                           COMMON::WorkSpace& p_space, const int node, const bool isInit, const float distBound) const {
                if (node < 0)
                {
                    int index = -node - 1;
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
                int otherChild, bestChild;
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
            void DivideTree(VectorIndex* p_index, std::vector<int>& indices, int first, int last,
                int index, int &iTreeSize) {
                ChooseDivision<T>(p_index, m_pTreeRoots[index], indices, first, last);
                int i = Subdivide<T>(p_index, m_pTreeRoots[index], indices, first, last);
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
            void ChooseDivision(VectorIndex* p_index, KDTNode& node, const std::vector<int>& indices, const int first, const int last)
            {
                std::vector<float> meanValues(p_index->GetFeatureDim(), 0);
                std::vector<float> varianceValues(p_index->GetFeatureDim(), 0);
                int end = min(first + m_iSamples, last);
                int count = end - first + 1;
                // calculate the mean of each dimension
                for (int j = first; j <= end; j++)
                {
                    const T* v = (const T*)p_index->GetSample(indices[j]);
                    for (int k = 0; k < p_index->GetFeatureDim(); k++)
                    {
                        meanValues[k] += v[k];
                    }
                }
                for (int k = 0; k < p_index->GetFeatureDim(); k++)
                {
                    meanValues[k] /= count;
                }
                // calculate the variance of each dimension
                for (int j = first; j <= end; j++)
                {
                    const T* v = (const T*)p_index->GetSample(indices[j]);
                    for (int k = 0; k < p_index->GetFeatureDim(); k++)
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

            int SelectDivisionDimension(const std::vector<float>& varianceValues) const
            {
                // Record the top maximum variances
                std::vector<int> topind(m_numTopDimensionKDTSplit);
                int num = 0;
                // order the variances
                for (int i = 0; i < varianceValues.size(); i++)
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
                return topind[COMMON::Utils::rand_int(num)];
            }

            template <typename T>
            int Subdivide(VectorIndex* p_index, const KDTNode& node, std::vector<int>& indices, const int first, const int last) const
            {
                int i = first;
                int j = last;
                // decide which child one point belongs
                while (i <= j)
                {
                    int ind = indices[i];
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
            std::vector<int> m_pTreeStart;
            std::vector<KDTNode> m_pTreeRoots;

        public:
            int m_iTreeNumber, m_numTopDimensionKDTSplit, m_iSamples;
        };
    }
}
#endif
