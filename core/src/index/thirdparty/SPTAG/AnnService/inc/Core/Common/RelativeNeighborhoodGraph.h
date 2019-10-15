// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_RNG_H_
#define _SPTAG_COMMON_RNG_H_

#include "NeighborhoodGraph.h"

namespace SPTAG
{
    namespace COMMON
    {
        class RelativeNeighborhoodGraph: public NeighborhoodGraph
        {
        public:
            void RebuildNeighbors(VectorIndex* index, const SizeType node, SizeType* nodes, const BasicResult* queryResults, const int numResults) {
                DimensionType count = 0;
                for (int j = 0; j < numResults && count < m_iNeighborhoodSize; j++) {
                    const BasicResult& item = queryResults[j];
                    if (item.VID < 0) break;
                    if (item.VID == node) continue;

                    bool good = true;
                    for (DimensionType k = 0; k < count; k++) {
                        if (index->ComputeDistance(index->GetSample(nodes[k]), index->GetSample(item.VID)) <= item.Dist) {
                            good = false;
                            break;
                        }
                    }
                    if (good) nodes[count++] = item.VID;
                }
                for (DimensionType j = count; j < m_iNeighborhoodSize; j++)  nodes[j] = -1;
            }

            void InsertNeighbors(VectorIndex* index, const SizeType node, SizeType insertNode, float insertDist)
            {
                SizeType* nodes = m_pNeighborhoodGraph[node];
                for (DimensionType k = 0; k < m_iNeighborhoodSize; k++)
                {
                    SizeType tmpNode = nodes[k];
                    if (tmpNode < -1) continue;

                    if (tmpNode < 0)
                    {
                        bool good = true;
                        for (DimensionType t = 0; t < k; t++) {
                            if (index->ComputeDistance(index->GetSample(insertNode), index->GetSample(nodes[t])) < insertDist) {
                                good = false;
                                break;
                            }
                        }
                        if (good) {
                            nodes[k] = insertNode;
                        }
                        break;
                    }
                    float tmpDist = index->ComputeDistance(index->GetSample(node), index->GetSample(tmpNode));
                    if (insertDist < tmpDist || (insertDist == tmpDist && insertNode < tmpNode))
                    {
                        bool good = true;
                        for (DimensionType t = 0; t < k; t++) {
                            if (index->ComputeDistance(index->GetSample(insertNode), index->GetSample(nodes[t])) < insertDist) {
                                good = false;
                                break;
                            }
                        }
                        if (good) {
                            nodes[k] = insertNode;
                            insertNode = tmpNode;
                            insertDist = tmpDist;
                        }
                        else {
                            break;
                        }
                    }
                }
            }

            float GraphAccuracyEstimation(VectorIndex* index, const SizeType samples, const std::unordered_map<SizeType, SizeType>* idmap = nullptr)
            {
                DimensionType* correct = new DimensionType[samples];

#pragma omp parallel for schedule(dynamic)
                for (SizeType i = 0; i < samples; i++)
                {
                    SizeType x = COMMON::Utils::rand(m_iGraphSize);
                    //int x = i;
                    COMMON::QueryResultSet<void> query(nullptr, m_iCEF);
                    for (SizeType y = 0; y < m_iGraphSize; y++)
                    {
                        if ((idmap != nullptr && idmap->find(y) != idmap->end())) continue;
                        float dist = index->ComputeDistance(index->GetSample(x), index->GetSample(y));
                        query.AddPoint(y, dist);
                    }
                    query.SortResult();
                    SizeType * exact_rng = new SizeType[m_iNeighborhoodSize];
                    RebuildNeighbors(index, x, exact_rng, query.GetResults(), m_iCEF);
                  
                    correct[i] = 0;
                    for (DimensionType j = 0; j < m_iNeighborhoodSize; j++) {
                        if (exact_rng[j] == -1) {
                            correct[i] += m_iNeighborhoodSize - j;
                            break;
                        }
                        for (DimensionType k = 0; k < m_iNeighborhoodSize; k++)
                            if ((m_pNeighborhoodGraph)[x][k] == exact_rng[j]) {
                                correct[i]++;
                                break;
                            }
                    }
                    delete[] exact_rng;
                }
                float acc = 0;
                for (SizeType i = 0; i < samples; i++) acc += float(correct[i]);
                acc = acc / samples / m_iNeighborhoodSize;
                delete[] correct;
                return acc;
            }

        };
    }
}
#endif