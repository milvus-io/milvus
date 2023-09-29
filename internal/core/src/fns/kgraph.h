// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef INTERNAL_CORE_SRC_FNS_KGRAPH_H_ 
#define INTERNAL_CORE_SRC_FNS_KGRAPH_H_ 

#include "kgraph_data.h"
#include <string>
#include <vector>

#include <omp.h>
#include <unordered_set>

#include <iostream>
#include <fstream>
#include <random>
#include <algorithm>
#include "boost/smart_ptr/detail/spinlock.hpp"
#include <mutex>


using namespace kgraph;

using Neighbors = std::vector<Neighbor>;
using graph = std::vector<std::vector<Neighbor>>;

typedef boost::detail::spinlock Lock;
typedef std::lock_guard<Lock> LockGuard;

struct IndexParams {
    unsigned iterations; 
    unsigned L;
    unsigned K;
    unsigned S;
    unsigned R;
    unsigned controls;
    unsigned seed;
    float delta;
    float recall;
    unsigned prune;
    int reverse;

    /// Construct with default values.
    IndexParams (): iterations(10), L(100), K(100), S(24), R(24), controls(0), seed(0), delta(0), recall(1), prune(0), reverse(0) {
    }
};




struct Nhood { // neighborhood
    Lock lock;
    float radius;   // distance of interesting range
    float radiusM;
    Neighbors pool;
    unsigned L;     // # valid items in the pool,  L + 1 <= pool.size()
    unsigned M;     // we only join items in pool[0..M)
    bool found;     // helped found new NN in this round
    std::vector<unsigned> nn_old;
    std::vector<unsigned> nn_new;
    std::vector<unsigned> rnn_old;
    std::vector<unsigned> rnn_new;

    unsigned UpdateKnnListHelper (Neighbor *addr, unsigned K, const Neighbor & nn) { /// why inline NO bug TODO: 
        // find the location to insert
        unsigned j;
        unsigned i = K;
        while (i > 0) {
            j = i - 1;
            if (addr[j].dist <= nn.dist) break;
            i = j;
        }
        // check for equal ID
        unsigned l = i;
        while (l > 0) {
            j = l - 1;
            if (addr[j].dist < nn.dist) break;
            if (addr[j].id == nn.id) return K + 1;
            l = j;
        }
        // i <= K-1
        j = K;
        while (j > i) {
            addr[j] = addr[j-1];
            --j;
        }
        addr[i] = nn;
        return i;
        return 0;
    }

    unsigned UpdateKnnList (Neighbor *addr, unsigned K, const Neighbor & nn) {
            return UpdateKnnListHelper(addr, K, nn);
    }


    unsigned parallel_try_insert (unsigned id, float dist) {
        if (dist > radius) return pool.size();
        LockGuard guard(lock);
        unsigned l = UpdateKnnList(&pool[0], L, Neighbor(id, dist, true));
        if (l <= L) { 
            if (L + 1 < pool.size()) { 
                ++L;
            }
            else {
                radius = pool[L-1].dist;
            }
        }
        return l;
    }


    template <typename C>
    void join (C callback) const {
        for (unsigned const i: nn_new) {
            for (unsigned const j: nn_new) {
                if (i < j) {
                    callback(i, j);
                }
            }
            for (unsigned j: nn_old) {
                callback(i, j);
            }
        }
    }
};



class KGraphConstructor
{

public:
    std::vector<Nhood> nhoods;

private:
    IndexOracle const &oracle;
    IndexParams params;
    size_t n_comps;

private:
    template <typename RNG> void GenRandom (RNG &rng, unsigned *addr, unsigned size, unsigned N);
    void init();
    void join();
    void update(); 
    

public:
    int build_index(); 
    void inner_save(std::string);
    float evaluate(vector<vector<unsigned int>> & );

    KGraphConstructor(IndexOracle const &o, IndexParams &);
    ~KGraphConstructor();

    public:
        inline int parseLine(char* line){
            // This assumes that a digit will be found and the line ends in " Kb".
            int i = strlen(line);
            const char* p = line;
            while (*p <'0' || *p > '9') p++;
            line[i-3] = '\0';
            i = atoi(p);
            return i;
        }
        double getMemoryUsage(){
            FILE* file = fopen("/proc/self/status", "r");
            int highwater_mark = -1;
            int current_memory = -1;
            char line[128];

            while (fgets(line, 128, file) != NULL){
                if (strncmp(line, "VmHWM:", 6) == 0){
                    highwater_mark = parseLine(line);
                }

                if (strncmp(line, "VmRSS:", 6) == 0){
                    current_memory = parseLine(line);
                }
                if (highwater_mark > 0 && current_memory > 0) {
                    break;
                }
            }
            fclose(file);
            return (double) 1.0 * highwater_mark / 1024;
        }
};




#endif  // INTERNAL_CORE_SRC_FNS_KGRAPH_H_
