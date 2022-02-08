#pragma once

#include "visited_list_pool.h"
#include "hnswlib.h"
#include <random>
#include <stdlib.h>
#include <unordered_set>
#include <list>

#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace hnswlib {

typedef unsigned int tableint;
typedef unsigned int linklistsizeint;


template<typename dist_t>
class HierarchicalNSW : public AlgorithmInterface<dist_t> {
 public:
    HierarchicalNSW(SpaceInterface<dist_t> *s) {
    }

    HierarchicalNSW(SpaceInterface<dist_t> *s, const std::string &location, bool nmslib = false, size_t max_elements=0) {
        loadIndex(location, s, max_elements);
    }

    HierarchicalNSW(SpaceInterface<dist_t> *s, size_t max_elements, size_t M = 16, size_t ef_construction = 200, size_t random_seed = 100) :
            link_list_locks_(max_elements), element_levels_(max_elements) {
        // linxj
        space = s;
        if (auto x = dynamic_cast<L2Space*>(s)) {
            metric_type_ = 0;
        } else if (auto x = dynamic_cast<InnerProductSpace*>(s)) {
            metric_type_ = 1;
        } else {
            metric_type_ = 100;
        }

        max_elements_ = max_elements;

        data_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();
        M_ = M;
        maxM_ = M_;
        maxM0_ = M_ * 2;
        ef_construction_ = std::max(ef_construction,M_);
        ef_ = 10;

        level_generator_.seed(random_seed);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        size_data_per_element_ = size_links_level0_ + data_size_; // + sizeof(labeltype);
        offsetData_ = size_links_level0_;
//        label_offset_ = size_links_level0_ + data_size_;
        offsetLevel0_ = 0;

        data_level0_memory_ = (char *) malloc(max_elements_ * size_data_per_element_);
        if (data_level0_memory_ == nullptr)
            throw std::runtime_error("Not enough memory");

        cur_element_count = 0;

        visited_list_pool_ = new VisitedListPool(1, max_elements);



        //initializations for special treatment of the first node
        enterpoint_node_ = -1;
        maxlevel_ = -1;

        linkLists_ = (char **) malloc(sizeof(void *) * max_elements_);
        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: HierarchicalNSW failed to allocate linklists");
        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
        mult_ = 1 / log(1.0 * M_);
        revSize_ = 1.0 / mult_;
        level_stats_.resize(10);
        stats_enable = false;
    }

    struct CompareByFirst {
        constexpr bool operator()(std::pair<dist_t, tableint> const &a,
                                  std::pair<dist_t, tableint> const &b) const noexcept {
            return a.first < b.first;
        }
    };

    ~HierarchicalNSW() {

        free(data_level0_memory_);
        for (tableint i = 0; i < cur_element_count; i++) {
            if (element_levels_[i] > 0)
                free(linkLists_[i]);
        }
        free(linkLists_);
        delete visited_list_pool_;

        // linxj: delete
        delete space;
    }

    // linxj: use for free resource
    SpaceInterface<dist_t> *space;
    size_t metric_type_; // 0:l2, 1:ip

    size_t max_elements_;
    size_t cur_element_count;
    size_t size_data_per_element_;
    size_t size_links_per_element_;

    size_t M_;
    size_t maxM_;
    size_t maxM0_;
    size_t ef_construction_;

    double mult_, revSize_;
    int maxlevel_;


    VisitedListPool *visited_list_pool_;
    std::mutex cur_element_count_guard_;

    std::vector<std::mutex> link_list_locks_;
    tableint enterpoint_node_;


    size_t size_links_level0_;
    size_t offsetData_, offsetLevel0_;


    char *data_level0_memory_;
    char **linkLists_;
    std::vector<int> element_levels_;
    std::vector<int> level_stats_;
    bool stats_enable = false;

    size_t data_size_;

    size_t label_offset_;
    DISTFUNC<dist_t> fstdistfunc_;
    void *dist_func_param_;

    std::default_random_engine level_generator_;

    inline char *getDataByInternalId(tableint internal_id) const {
        return (data_level0_memory_ + internal_id * size_data_per_element_ + offsetData_);
    }

    int getRandomLevel(double reverse_size) {
        std::uniform_real_distribution<double> distribution(0.0, 1.0);
        double r = -log(distribution(level_generator_)) * reverse_size;
        return (int) r;
    }

    std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
    searchBaseLayer(tableint ep_id, const void *data_point, int layer) {
        VisitedList *vl = visited_list_pool_->getFreeVisitedList();
        vl_type *visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates;
        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> candidateSet;

        dist_t lowerBound;
        dist_t dist = fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
        top_candidates.emplace(dist, ep_id);
        lowerBound = dist;
        candidateSet.emplace(-dist, ep_id);
        visited_array[ep_id] = visited_array_tag;

        while (!candidateSet.empty()) {
            std::pair<dist_t, tableint> curr_el_pair = candidateSet.top();
            if ((-curr_el_pair.first) > lowerBound) {
                break;
            }
            candidateSet.pop();

            tableint curNodeNum = curr_el_pair.second;

            std::unique_lock <std::mutex> lock(link_list_locks_[curNodeNum]);

            int *data;// = (int *)(linkList0_ + curNodeNum * size_links_per_element0_);
            if (layer == 0) {
                data = (int*)get_linklist0(curNodeNum);
            } else {
                data = (int*)get_linklist(curNodeNum, layer);
                // data = (int *) (linkLists_[curNodeNum] + (layer - 1) * size_links_per_element_);
            }
            size_t size = getListCount((linklistsizeint*)data);
            tableint *datal = (tableint *) (data + 1);
#ifdef USE_SSE
            _mm_prefetch((char *) (visited_array + *(data + 1)), _MM_HINT_T0);
            _mm_prefetch((char *) (visited_array + *(data + 1) + 64), _MM_HINT_T0);
            _mm_prefetch(getDataByInternalId(*datal), _MM_HINT_T0);
            _mm_prefetch(getDataByInternalId(*(datal + 1)), _MM_HINT_T0);
#endif

            for (size_t j = 0; j < size; j++) {
                tableint candidate_id = *(datal + j);
                // if (candidate_id == 0) continue;
#ifdef USE_SSE
                _mm_prefetch((char *) (visited_array + *(datal + j + 1)), _MM_HINT_T0);
                _mm_prefetch(getDataByInternalId(*(datal + j + 1)), _MM_HINT_T0);
#endif
                if (visited_array[candidate_id] == visited_array_tag) continue;
                visited_array[candidate_id] = visited_array_tag;
                char *currObj1 = (getDataByInternalId(candidate_id));

                dist_t dist1 = fstdistfunc_(data_point, currObj1, dist_func_param_);
                if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
                    candidateSet.emplace(-dist1, candidate_id);
#ifdef USE_SSE
                    _mm_prefetch(getDataByInternalId(candidateSet.top().second), _MM_HINT_T0);
#endif

                    top_candidates.emplace(dist1, candidate_id);

                    if (top_candidates.size() > ef_construction_)
                        top_candidates.pop();

                    if (!top_candidates.empty())
                        lowerBound = top_candidates.top().first;
                }
            }
        }
        visited_list_pool_->releaseVisitedList(vl);

        return top_candidates;
    }

    template <bool has_deletions>
    std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
    searchBaseLayerST(tableint ep_id, const void *data_point, size_t ef, const faiss::BitsetView bitset, StatisticsInfo &stats) const {
        VisitedList *vl = visited_list_pool_->getFreeVisitedList();
        vl_type *visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates;
        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> candidate_set;

        dist_t lowerBound;
//        if (!has_deletions || !isMarkedDeleted(ep_id)) {
          if (!has_deletions || !bitset.test((int64_t)ep_id)) {
            dist_t dist = fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
            lowerBound = dist;
            top_candidates.emplace(dist, ep_id);
            candidate_set.emplace(-dist, ep_id);
        } else {
            lowerBound = std::numeric_limits<dist_t>::max();
            candidate_set.emplace(-lowerBound, ep_id);
        }

        visited_array[ep_id] = visited_array_tag;

        while (!candidate_set.empty()) {

            std::pair<dist_t, tableint> current_node_pair = candidate_set.top();

            if ((-current_node_pair.first) > lowerBound) {
                break;
            }
            candidate_set.pop();

            tableint current_node_id = current_node_pair.second;
            int *data = (int *) get_linklist0(current_node_id);
            size_t size = getListCount((linklistsizeint*)data);
            // bool cur_node_deleted = isMarkedDeleted(current_node_id);

#ifdef USE_SSE
            _mm_prefetch((char *) (visited_array + *(data + 1)), _MM_HINT_T0);
            _mm_prefetch((char *) (visited_array + *(data + 1) + 64), _MM_HINT_T0);
            _mm_prefetch(data_level0_memory_ + (*(data + 1)) * size_data_per_element_ + offsetData_, _MM_HINT_T0);
            _mm_prefetch((char *) (data + 2), _MM_HINT_T0);
#endif

            for (size_t j = 1; j <= size; j++) {
                int candidate_id = *(data + j);
                // if (candidate_id == 0) continue;
#ifdef USE_SSE
                _mm_prefetch((char *) (visited_array + *(data + j + 1)), _MM_HINT_T0);
                _mm_prefetch(data_level0_memory_ + (*(data + j + 1)) * size_data_per_element_ + offsetData_,
                             _MM_HINT_T0);////////////
#endif
                if (!(visited_array[candidate_id] == visited_array_tag)) {

                    visited_array[candidate_id] = visited_array_tag;

                    char *currObj1 = (getDataByInternalId(candidate_id));
                    dist_t dist = fstdistfunc_(data_point, currObj1, dist_func_param_);

                    if (top_candidates.size() < ef || lowerBound > dist) {
                        candidate_set.emplace(-dist, candidate_id);
#ifdef USE_SSE
                        _mm_prefetch(data_level0_memory_ + candidate_set.top().second * size_data_per_element_ +
                                     offsetLevel0_,///////////
                                     _MM_HINT_T0);////////////////////////
#endif

//                        if (!has_deletions || !isMarkedDeleted(candidate_id))
                        if (!has_deletions || (!bitset.test((int64_t)candidate_id))) {
                            top_candidates.emplace(dist, candidate_id);
                        }

                        if (top_candidates.size() > ef)
                            top_candidates.pop();

                        if (!top_candidates.empty())
                            lowerBound = top_candidates.top().first;
                    }
                }
            }
        }

        visited_list_pool_->releaseVisitedList(vl);
        return top_candidates;
    }

    std::vector<tableint>
    getNeighborsByHeuristic2 (
            std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> &top_candidates,
            const size_t M) {
        std::vector<tableint> return_list;

        if (top_candidates.size() < M) {
            return_list.resize(top_candidates.size());

            for (int i = static_cast<int>(top_candidates.size() - 1); i >= 0; i--) {
                return_list[i] = top_candidates.top().second;
                top_candidates.pop();
            }

        } else if (M > 0) {
            return_list.reserve(M);

            std::vector<std::pair<dist_t, tableint>> queue_closest;
            queue_closest.resize(top_candidates.size());
            for (int i = static_cast<int>(top_candidates.size() - 1); i >= 0; i--) {
                queue_closest[i] = top_candidates.top();
                top_candidates.pop();
            }

            for (std::pair<dist_t, tableint> &current_pair: queue_closest) {
                bool good = true;
                for (tableint id : return_list) {
                    dist_t curdist =
                            fstdistfunc_(getDataByInternalId(id),
                                         getDataByInternalId(current_pair.second),
                                         dist_func_param_);
                    if (curdist < current_pair.first) {
                        good = false;
                        break;
                    }
                }
                if (good) {
                    return_list.push_back(current_pair.second);
                    if (return_list.size() >= M) {
                        break;
                    }
                }
            }
        }

        return return_list;
    }

    linklistsizeint *get_linklist0(tableint internal_id) const {
        return (linklistsizeint *) (data_level0_memory_ + internal_id * size_data_per_element_ + offsetLevel0_);
    };

    linklistsizeint *get_linklist0(tableint internal_id, char *data_level0_memory_) const {
        return (linklistsizeint *) (data_level0_memory_ + internal_id * size_data_per_element_ + offsetLevel0_);
    };

    linklistsizeint *get_linklist(tableint internal_id, int level) const {
        return (linklistsizeint *) (linkLists_[internal_id] + (level - 1) * size_links_per_element_);
    };

    tableint mutuallyConnectNewElement(const void *data_point, tableint cur_c,
                                       std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> &top_candidates,
                                       int level) {

        size_t Mcurmax = level ? maxM_ : maxM0_;

        std::vector<tableint> selectedNeighbors(getNeighborsByHeuristic2(top_candidates, M_));
        if (selectedNeighbors.size() > M_)
            throw std::runtime_error("Should be not be more than M_ candidates returned by the heuristic");

        tableint next_closest_entry_point = selectedNeighbors.front();

        {
            linklistsizeint *ll_cur;
            if (level == 0)
                ll_cur = get_linklist0(cur_c);
            else
                ll_cur = get_linklist(cur_c, level);

            if (*ll_cur) {
                throw std::runtime_error("The newly inserted element should have blank link list");
            }
            setListCount(ll_cur,selectedNeighbors.size());
            tableint *data = (tableint *) (ll_cur + 1);


            for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
                if (data[idx])
                    throw std::runtime_error("Possible memory corruption");
                if (level > element_levels_[selectedNeighbors[idx]])
                    throw std::runtime_error("Trying to make a link on a non-existent level");

                data[idx] = selectedNeighbors[idx];

            }
        }
        for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {

            std::unique_lock <std::mutex> lock(link_list_locks_[selectedNeighbors[idx]]);


            linklistsizeint *ll_other;
            if (level == 0)
                ll_other = get_linklist0(selectedNeighbors[idx]);
            else
                ll_other = get_linklist(selectedNeighbors[idx], level);

            size_t sz_link_list_other = getListCount(ll_other);

            if (sz_link_list_other > Mcurmax)
                throw std::runtime_error("Bad value of sz_link_list_other");
            if (selectedNeighbors[idx] == cur_c)
                throw std::runtime_error("Trying to connect an element to itself");
            if (level > element_levels_[selectedNeighbors[idx]])
                throw std::runtime_error("Trying to make a link on a non-existent level");

            tableint *data = (tableint *) (ll_other + 1);
            if (sz_link_list_other < Mcurmax) {
                data[sz_link_list_other] = cur_c;
                setListCount(ll_other, sz_link_list_other + 1);
            } else {
                // finding the "weakest" element to replace it with the new one
                dist_t d_max = fstdistfunc_(getDataByInternalId(cur_c), getDataByInternalId(selectedNeighbors[idx]),
                                            dist_func_param_);
                // Heuristic:
                std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> candidates;
                candidates.emplace(d_max, cur_c);

                for (size_t j = 0; j < sz_link_list_other; j++) {
                    candidates.emplace(
                            fstdistfunc_(getDataByInternalId(data[j]), getDataByInternalId(selectedNeighbors[idx]),
                                         dist_func_param_), data[j]);
                }

                std::vector<tableint> selected(getNeighborsByHeuristic2(candidates, Mcurmax));
                setListCount(ll_other, static_cast<unsigned short int>(selected.size()));
                for (size_t idx = 0; idx < selected.size(); idx++) {
                    data[idx] = selected[idx];
                }
                // Nearest K:
                /*int indx = -1;
                for (int j = 0; j < sz_link_list_other; j++) {
                    dist_t d = fstdistfunc_(getDataByInternalId(data[j]), getDataByInternalId(rez[idx]), dist_func_param_);
                    if (d > d_max) {
                        indx = j;
                        d_max = d;
                    }
                }
                if (indx >= 0) {
                    data[indx] = cur_c;
                } */
            }

        }

        return next_closest_entry_point;
    }

    std::mutex global;
    size_t ef_;

    void setEf(size_t ef) {
        ef_ = ef;
    }

    void resizeIndex(size_t new_max_elements){
        if (new_max_elements<cur_element_count)
            throw std::runtime_error("Cannot resize, max element is less than the current number of elements");


        delete visited_list_pool_;
        visited_list_pool_ = new VisitedListPool(1, new_max_elements);



        element_levels_.resize(new_max_elements);

        std::vector<std::mutex>(new_max_elements).swap(link_list_locks_);


        char * data_level0_memory_new = (char *) realloc(data_level0_memory_, new_max_elements * size_data_per_element_);
        if (data_level0_memory_new == nullptr)
            throw std::runtime_error("Not enough memory: resizeIndex failed to allocate base layer");
        data_level0_memory_ = data_level0_memory_new;

        // Reallocate all other layers
        char ** linkLists_new = (char **) realloc(linkLists_, sizeof(void *) * new_max_elements);
        if (linkLists_new == nullptr)
            throw std::runtime_error("Not enough memory: resizeIndex failed to allocate other layers");
        linkLists_ = linkLists_new;

        max_elements_ = new_max_elements;

    }

    void saveIndex(milvus::knowhere::MemoryIOWriter& output) {
        // write l2/ip calculator
        writeBinaryPOD(output, metric_type_);
        writeBinaryPOD(output, data_size_);
        writeBinaryPOD(output, *((size_t *) dist_func_param_));

        writeBinaryPOD(output, offsetLevel0_);
        writeBinaryPOD(output, max_elements_);
        writeBinaryPOD(output, cur_element_count);
        writeBinaryPOD(output, size_data_per_element_);
        writeBinaryPOD(output, label_offset_);
        writeBinaryPOD(output, offsetData_);
        writeBinaryPOD(output, maxlevel_);
        writeBinaryPOD(output, enterpoint_node_);
        writeBinaryPOD(output, maxM_);

        writeBinaryPOD(output, maxM0_);
        writeBinaryPOD(output, M_);
        writeBinaryPOD(output, mult_);
        writeBinaryPOD(output, ef_construction_);

        output.write(data_level0_memory_, cur_element_count * size_data_per_element_);

        for (size_t i = 0; i < cur_element_count; i++) {
            unsigned int linkListSize = element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
            writeBinaryPOD(output, linkListSize);
            if (linkListSize)
                output.write(linkLists_[i], linkListSize);
        }
        // output.close();
    }

    void loadIndex(milvus::knowhere::MemoryIOReader& input, size_t max_elements_i = 0) {
        // linxj: init with metrictype
        size_t dim = 100;
        readBinaryPOD(input, metric_type_);
        readBinaryPOD(input, data_size_);
        readBinaryPOD(input, dim);
        if (metric_type_ == 0) {
            space = new hnswlib::L2Space(dim);
        } else if (metric_type_ == 1) {
            space = new hnswlib::InnerProductSpace(dim);
        } else {
            // throw exception
        }
        fstdistfunc_ = space->get_dist_func();
        dist_func_param_ = space->get_dist_func_param();

        readBinaryPOD(input, offsetLevel0_);
        readBinaryPOD(input, max_elements_);
        readBinaryPOD(input, cur_element_count);

        size_t max_elements=max_elements_i;
        if(max_elements < cur_element_count)
            max_elements = max_elements_;
        max_elements_ = max_elements;
        readBinaryPOD(input, size_data_per_element_);
        readBinaryPOD(input, label_offset_);
        readBinaryPOD(input, offsetData_);
        readBinaryPOD(input, maxlevel_);
        readBinaryPOD(input, enterpoint_node_);

        readBinaryPOD(input, maxM_);
        readBinaryPOD(input, maxM0_);
        readBinaryPOD(input, M_);
        readBinaryPOD(input, mult_);
        readBinaryPOD(input, ef_construction_);


        // data_size_ = s->get_data_size();
        // fstdistfunc_ = s->get_dist_func();
        // dist_func_param_ = s->get_dist_func_param();

        // auto pos= input.rp;


        // /// Optional - check if index is ok:
        //
        // input.seekg(cur_element_count * size_data_per_element_,input.cur);
        // for (size_t i = 0; i < cur_element_count; i++) {
        //     if(input.tellg() < 0 || input.tellg()>=total_filesize){
        //         throw std::runtime_error("Index seems to be corrupted or unsupported");
        //     }
        //
        //     unsigned int linkListSize;
        //     readBinaryPOD(input, linkListSize);
        //     if (linkListSize != 0) {
        //         input.seekg(linkListSize,input.cur);
        //     }
        // }
        //
        // // throw exception if it either corrupted or old index
        // if(input.tellg()!=total_filesize)
        //     throw std::runtime_error("Index seems to be corrupted or unsupported");
        //
        // input.clear();
        //
        // /// Optional check end
        //
        // input.seekg(pos,input.beg);


        data_level0_memory_ = (char *) malloc(max_elements * size_data_per_element_);
        if (data_level0_memory_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate level0");
        input.read(data_level0_memory_, cur_element_count * size_data_per_element_);




        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);


        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        std::vector<std::mutex>(max_elements).swap(link_list_locks_);


        visited_list_pool_ = new VisitedListPool(1, max_elements);


        linkLists_ = (char **) malloc(sizeof(void *) * max_elements);
        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklists");
        element_levels_ = std::vector<int>(max_elements);
        if (stats_enable)
            level_stats_ = std::vector<int>(maxlevel_ + 1, 0);
        revSize_ = 1.0 / mult_;
        ef_ = 10;
        for (size_t i = 0; i < cur_element_count; i++) {
            unsigned int linkListSize;
            readBinaryPOD(input, linkListSize);
            if (linkListSize == 0) {
                element_levels_[i] = 0;
                if (stats_enable)
                    level_stats_[0] ++;

                linkLists_[i] = nullptr;
            } else {
                element_levels_[i] = linkListSize / size_links_per_element_;
                if (stats_enable)
                    level_stats_[element_levels_[i]] ++;
                linkLists_[i] = (char *) malloc(linkListSize);
                if (linkLists_[i] == nullptr)
                    throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklist");
                input.read(linkLists_[i], linkListSize);
            }
        }
    }

    void saveIndex(const std::string &location) {
        std::ofstream output(location, std::ios::binary);
        std::streampos position;

        writeBinaryPOD(output, offsetLevel0_);
        writeBinaryPOD(output, max_elements_);
        writeBinaryPOD(output, cur_element_count);
        writeBinaryPOD(output, size_data_per_element_);
        writeBinaryPOD(output, label_offset_);
        writeBinaryPOD(output, offsetData_);
        writeBinaryPOD(output, maxlevel_);
        writeBinaryPOD(output, enterpoint_node_);
        writeBinaryPOD(output, maxM_);

        writeBinaryPOD(output, maxM0_);
        writeBinaryPOD(output, M_);
        writeBinaryPOD(output, mult_);
        writeBinaryPOD(output, ef_construction_);

        output.write(data_level0_memory_, cur_element_count * size_data_per_element_);

        for (size_t i = 0; i < cur_element_count; i++) {
            unsigned int linkListSize = element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
            writeBinaryPOD(output, linkListSize);
            if (linkListSize)
                output.write(linkLists_[i], linkListSize);
        }
        output.close();
    }

    void loadIndex(const std::string &location, SpaceInterface<dist_t> *s, size_t max_elements_i=0) {
        std::ifstream input(location, std::ios::binary);

        if (!input.is_open())
            throw std::runtime_error("Cannot open file");

        // get file size:
        input.seekg(0,input.end);
        std::streampos total_filesize=input.tellg();
        input.seekg(0,input.beg);

        readBinaryPOD(input, offsetLevel0_);
        readBinaryPOD(input, max_elements_);
        readBinaryPOD(input, cur_element_count);

        size_t max_elements=max_elements_i;
        if(max_elements < cur_element_count)
            max_elements = max_elements_;
        max_elements_ = max_elements;
        readBinaryPOD(input, size_data_per_element_);
        readBinaryPOD(input, label_offset_);
        readBinaryPOD(input, offsetData_);
        readBinaryPOD(input, maxlevel_);
        readBinaryPOD(input, enterpoint_node_);

        readBinaryPOD(input, maxM_);
        readBinaryPOD(input, maxM0_);
        readBinaryPOD(input, M_);
        readBinaryPOD(input, mult_);
        readBinaryPOD(input, ef_construction_);

        data_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();

        auto pos=input.tellg();

        /// Optional - check if index is ok:

        input.seekg(cur_element_count * size_data_per_element_,input.cur);
        for (size_t i = 0; i < cur_element_count; i++) {
            if(input.tellg() < 0 || input.tellg()>=total_filesize){
                throw std::runtime_error("Index seems to be corrupted or unsupported");
            }

            unsigned int linkListSize;
            readBinaryPOD(input, linkListSize);
            if (linkListSize != 0) {
                input.seekg(linkListSize,input.cur);
            }
        }

        // throw exception if it either corrupted or old index
        if(input.tellg()!=total_filesize)
            throw std::runtime_error("Index seems to be corrupted or unsupported");

        input.clear();

        /// Optional check end

        input.seekg(pos,input.beg);

        data_level0_memory_ = (char *) malloc(max_elements * size_data_per_element_);
        if (data_level0_memory_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate level0");
        input.read(data_level0_memory_, cur_element_count * size_data_per_element_);

        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        std::vector<std::mutex>(max_elements).swap(link_list_locks_);

        visited_list_pool_ = new VisitedListPool(1, max_elements);

        linkLists_ = (char **) malloc(sizeof(void *) * max_elements);
        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklists");
        element_levels_ = std::vector<int>(max_elements);
        revSize_ = 1.0 / mult_;
        ef_ = 10;
        for (size_t i = 0; i < cur_element_count; i++) {
            unsigned int linkListSize;
            readBinaryPOD(input, linkListSize);
            if (linkListSize == 0) {
                element_levels_[i] = 0;
                linkLists_[i] = nullptr;
            } else {
                element_levels_[i] = linkListSize / size_links_per_element_;
                linkLists_[i] = (char *) malloc(linkListSize);
                if (linkLists_[i] == nullptr)
                    throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklist");
                input.read(linkLists_[i], linkListSize);
            }
        }

        input.close();
    }

    unsigned short int getListCount(linklistsizeint * ptr) const {
        return *((unsigned short int *)ptr);
    }

    void setListCount(linklistsizeint * ptr, unsigned short int size) const {
        *((unsigned short int*)(ptr))=*((unsigned short int *)&size);
    }

    void addPoint(const void *data_point, labeltype label) {
        addPoint(data_point, label, -1);
    }

    tableint addPoint(const void *data_point, labeltype label, int level) {
        tableint cur_c = label;
        {
            std::unique_lock <std::mutex> lock(cur_element_count_guard_);
            if (cur_element_count >= max_elements_) {
                throw std::runtime_error("The number of elements exceeds the specified limit");
            };

            cur_element_count++;
        }

        std::unique_lock <std::mutex> lock_el(link_list_locks_[cur_c]);
        int curlevel = (level > 0) ? level : getRandomLevel(mult_);

        element_levels_[cur_c] = curlevel;

        std::unique_lock <std::mutex> templock(global);
        if (stats_enable) {
            if (curlevel >= level_stats_.size()) {
                level_stats_.resize(curlevel + 1, 0);
            }
            level_stats_[curlevel] ++;
        }
        int maxlevelcopy = maxlevel_;
        if (curlevel <= maxlevelcopy)
            templock.unlock();
        tableint currObj = enterpoint_node_;
        tableint enterpoint_copy = enterpoint_node_;

        memset(data_level0_memory_ + cur_c * size_data_per_element_ + offsetLevel0_, 0, size_data_per_element_);

        memcpy(getDataByInternalId(cur_c), data_point, data_size_);

        if (curlevel) {
            linkLists_[cur_c] = (char *) malloc(size_links_per_element_ * curlevel + 1);
            if (linkLists_[cur_c] == nullptr)
                throw std::runtime_error("Not enough memory: addPoint failed to allocate linklist");
            memset(linkLists_[cur_c], 0, size_links_per_element_ * curlevel + 1);
        }

        if ((signed)currObj != -1) {

            if (curlevel < maxlevelcopy) {

                dist_t curdist = fstdistfunc_(data_point, getDataByInternalId(currObj), dist_func_param_);
                for (int level = maxlevelcopy; level > curlevel; level--) {
                    bool changed = true;
                    while (changed) {
                        changed = false;
                        unsigned int *data;
                        std::unique_lock <std::mutex> lock(link_list_locks_[currObj]);
                        data = get_linklist(currObj,level);
                        int size = getListCount(data);

                        tableint *datal = (tableint *) (data + 1);
                        for (int i = 0; i < size; i++) {
                            tableint cand = datal[i];
                            if (cand < 0 || cand > max_elements_)
                                throw std::runtime_error("cand error");
                            dist_t d = fstdistfunc_(data_point, getDataByInternalId(cand), dist_func_param_);
                            if (d < curdist) {
                                curdist = d;
                                currObj = cand;
                                changed = true;
                            }
                        }
                    }
                }
            }

            for (int level = std::min(curlevel, maxlevelcopy); level >= 0; level--) {
                if (level > maxlevelcopy || level < 0)  // possible?
                    throw std::runtime_error("Level error");

                std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates = searchBaseLayer(
                        currObj, data_point, level);

                currObj = mutuallyConnectNewElement(data_point, cur_c, top_candidates, level);
            }
        } else {
            // Do nothing for the first element
            enterpoint_node_ = 0;
            maxlevel_ = curlevel;
        }

        //Releasing lock for the maximum level
        if (curlevel > maxlevelcopy) {
            enterpoint_node_ = cur_c;
            maxlevel_ = curlevel;
        }
        return cur_c;
    };

    std::priority_queue<std::pair<dist_t, labeltype >>
    searchKnn(const void *query_data, size_t k, const faiss::BitsetView bitset, StatisticsInfo &stats) const {
        std::priority_queue<std::pair<dist_t, labeltype >> result;
        if (cur_element_count == 0) return result;

        tableint currObj = enterpoint_node_;
        dist_t curdist = fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), dist_func_param_);

        for (int level = maxlevel_; level > 0; level--) {
            bool changed = true;
            while (changed) {
                changed = false;
                unsigned int *data;

                data = (unsigned int *) get_linklist(currObj, level);
                int size = getListCount(data);
                tableint *datal = (tableint *) (data + 1);
                for (int i = 0; i < size; i++) {
                    tableint cand = datal[i];
                    if (cand < 0 || cand > max_elements_)
                        throw std::runtime_error("cand error");
                    if (stats_enable && level == stats.target_level) {
                        stats.accessed_points.push_back(cand);
                    }
                    dist_t d = fstdistfunc_(query_data, getDataByInternalId(cand), dist_func_param_);

                    if (d < curdist) {
                        curdist = d;
                        currObj = cand;
                        changed = true;
                    }
                }
            }
        }

        std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst> top_candidates;
        if (!bitset.empty()) {
            std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
                top_candidates1 = searchBaseLayerST<true>(currObj, query_data, std::max(ef_, k), bitset, stats);
            top_candidates.swap(top_candidates1);
        }
        else{
            std::priority_queue<std::pair<dist_t, tableint>, std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
                top_candidates1 = searchBaseLayerST<false>(currObj, query_data, std::max(ef_, k), bitset, stats);
            top_candidates.swap(top_candidates1);
        }
        while (top_candidates.size() > k) {
            top_candidates.pop();
        }
        while (top_candidates.size() > 0) {
            std::pair<dist_t, tableint> rez = top_candidates.top();
            result.push(std::pair<dist_t, labeltype>(rez.first, rez.second));
            top_candidates.pop();
        }
        return result;
    };

    int64_t cal_size() {
        int64_t ret = 0;
        ret += sizeof(*this);
        ret += sizeof(*space);
        ret += visited_list_pool_->GetSize();
        ret += link_list_locks_.size() * sizeof(std::mutex);
        ret += element_levels_.size() * sizeof(int);
        ret += max_elements_ * size_data_per_element_;
        ret += max_elements_ * sizeof(void*);
        for (auto i = 0; i < max_elements_; ++ i) {
            if (element_levels_[i] > 0) {
                ret += size_links_per_element_ * element_levels_[i];
            }
        }
        return ret;
     }

    };

}





