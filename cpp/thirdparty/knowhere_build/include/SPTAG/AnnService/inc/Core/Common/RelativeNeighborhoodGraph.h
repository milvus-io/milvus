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
            void RebuildNeighbors(VectorIndex* index, const int node, int* nodes, const BasicResult* queryResults, const int numResults) {
                int count = 0;
                for (int j = 0; j < numResults && count < m_iNeighborhoodSize; j++) {
                    const BasicResult& item = queryResults[j];
                    if (item.VID < 0) break;
                    if (item.VID == node) continue;

                    bool good = true;
                    for (int k = 0; k < count; k++) {
                        if (index->ComputeDistance(index->GetSample(nodes[k]), index->GetSample(item.VID)) <= item.Dist) {
                            good = false;
                            break;
                        }
                    }
                    if (good) nodes[count++] = item.VID;
                }
                for (int j = count; j < m_iNeighborhoodSize; j++)  nodes[j] = -1;
            }

            void InsertNeighbors(VectorIndex* index, const int node, int insertNode, float insertDist)
            {
                int* nodes = m_pNeighborhoodGraph[node];
                for (int k = 0; k < m_iNeighborhoodSize; k++)
                {
                    int tmpNode = nodes[k];
                    if (tmpNode < -1) continue;

                    if (tmpNode < 0)
                    {
                        bool good = true;
                        for (int t = 0; t < k; t++) {
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
                        for (int t = 0; t < k; t++) {
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

            float GraphAccuracyEstimation(VectorIndex* index, const int samples, const std::unordered_map<int, int>* idmap = nullptr)
            {
                int* correct = new int[samples];

#pragma omp parallel for schedule(dynamic)
                for (int i = 0; i < samples; i++)
                {
                    int x = COMMON::Utils::rand_int(m_iGraphSize);
                    //int x = i;
                    COMMON::QueryResultSet<void> query(nullptr, m_iCEF);
                    for (int y = 0; y < m_iGraphSize; y++)
                    {
                        if ((idmap != nullptr && idmap->find(y) != idmap->end())) continue;
                        float dist = index->ComputeDistance(index->GetSample(x), index->GetSample(y));
                        query.AddPoint(y, dist);
                    }
                    query.SortResult();
                    int * exact_rng = new int[m_iNeighborhoodSize];
                    RebuildNeighbors(index, x, exact_rng, query.GetResults(), m_iCEF);
                  
                    correct[i] = 0;
                    for (int j = 0; j < m_iNeighborhoodSize; j++) {
                        if (exact_rng[j] == -1) {
                            correct[i] += m_iNeighborhoodSize - j;
                            break;
                        }
                        for (int k = 0; k < m_iNeighborhoodSize; k++)
                            if ((m_pNeighborhoodGraph)[x][k] == exact_rng[j]) {
                                correct[i]++;
                                break;
                            }
                    }
                    delete[] exact_rng;
                }
                float acc = 0;
                for (int i = 0; i < samples; i++) acc += float(correct[i]);
                acc = acc / samples / m_iNeighborhoodSize;
                delete[] correct;
                return acc;
            }

        };
    }
}
#endif