/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IVFlib.h>

#include <memory>

#include <faiss/IndexPreTransform.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/MetaIndexes.h>



namespace faiss { namespace ivflib {


void check_compatible_for_merge (const Index * index0,
                                 const Index * index1)
{

    const faiss::IndexPreTransform *pt0 =
        dynamic_cast<const faiss::IndexPreTransform *>(index0);

    if (pt0) {
        const faiss::IndexPreTransform *pt1 =
            dynamic_cast<const faiss::IndexPreTransform *>(index1);
        FAISS_THROW_IF_NOT_MSG (pt1, "both indexes should be pretransforms");

        FAISS_THROW_IF_NOT (pt0->chain.size() == pt1->chain.size());
        for (int i = 0; i < pt0->chain.size(); i++) {
            FAISS_THROW_IF_NOT (typeid(pt0->chain[i]) == typeid(pt1->chain[i]));
        }

        index0 = pt0->index;
        index1 = pt1->index;
    }
    FAISS_THROW_IF_NOT (typeid(index0) == typeid(index1));
    FAISS_THROW_IF_NOT (index0->d == index1->d &&
                        index0->metric_type == index1->metric_type);

    const faiss::IndexIVF *ivf0 = dynamic_cast<const faiss::IndexIVF *>(index0);
    if (ivf0) {
        const faiss::IndexIVF *ivf1 =
            dynamic_cast<const faiss::IndexIVF *>(index1);
        FAISS_THROW_IF_NOT (ivf1);

        ivf0->check_compatible_for_merge (*ivf1);
    }

    // TODO: check as thoroughfully for other index types

}

const IndexIVF * try_extract_index_ivf (const Index * index)
{
    if (auto *pt =
        dynamic_cast<const IndexPreTransform *>(index)) {
        index = pt->index;
    }

    if (auto *idmap =
        dynamic_cast<const IndexIDMap *>(index)) {
        index = idmap->index;
    }
    if (auto *idmap =
        dynamic_cast<const IndexIDMap2 *>(index)) {
        index = idmap->index;
    }

    auto *ivf = dynamic_cast<const IndexIVF *>(index);

    return ivf;
}

IndexIVF * try_extract_index_ivf (Index * index) {
    return const_cast<IndexIVF*> (try_extract_index_ivf ((const Index*)(index)));
}

const IndexIVF * extract_index_ivf (const Index * index)
{
    const IndexIVF *ivf = try_extract_index_ivf (index);
    FAISS_THROW_IF_NOT (ivf);
    return ivf;
}

IndexIVF * extract_index_ivf (Index * index) {
    return const_cast<IndexIVF*> (extract_index_ivf ((const Index*)(index)));
}


void merge_into(faiss::Index *index0, faiss::Index *index1, bool shift_ids) {

    check_compatible_for_merge (index0, index1);
    IndexIVF * ivf0 = extract_index_ivf (index0);
    IndexIVF * ivf1 = extract_index_ivf (index1);

    ivf0->merge_from (*ivf1, shift_ids ? ivf0->ntotal : 0);

    // useful for IndexPreTransform
    index0->ntotal = ivf0->ntotal;
    index1->ntotal = ivf1->ntotal;
}



void search_centroid(faiss::Index *index,
                     const float* x, int n,
                     idx_t* centroid_ids)
{
    std::unique_ptr<float[]> del;
    if (auto index_pre = dynamic_cast<faiss::IndexPreTransform*>(index)) {
        x = index_pre->apply_chain(n, x);
        del.reset((float*)x);
        index = index_pre->index;
    }
    faiss::IndexIVF* index_ivf = dynamic_cast<faiss::IndexIVF*>(index);
    assert(index_ivf);
    index_ivf->quantizer->assign(n, x, centroid_ids);
}



void search_and_return_centroids(faiss::Index *index,
                                 size_t n,
                                 const float* xin,
                                 long k,
                                 float *distances,
                                 idx_t* labels,
                                 idx_t* query_centroid_ids,
                                 idx_t* result_centroid_ids)
{
    const float *x = xin;
    std::unique_ptr<float []> del;
    if (auto index_pre = dynamic_cast<faiss::IndexPreTransform*>(index)) {
        x = index_pre->apply_chain(n, x);
        del.reset((float*)x);
        index = index_pre->index;
    }
    faiss::IndexIVF* index_ivf = dynamic_cast<faiss::IndexIVF*>(index);
    assert(index_ivf);

    size_t nprobe = index_ivf->nprobe;
    std::vector<idx_t> cent_nos (n * nprobe);
    std::vector<float> cent_dis (n * nprobe);
    index_ivf->quantizer->search(
        n, x, nprobe, cent_dis.data(), cent_nos.data());

    if (query_centroid_ids) {
        for (size_t i = 0; i < n; i++)
            query_centroid_ids[i] = cent_nos[i * nprobe];
    }

    index_ivf->search_preassigned (n, x, k,
                                   cent_nos.data(), cent_dis.data(),
                                   distances, labels, true);

    for (size_t i = 0; i < n * k; i++) {
        idx_t label = labels[i];
        if (label < 0) {
            if (result_centroid_ids)
                result_centroid_ids[i] = -1;
        } else {
            long list_no = lo_listno (label);
            long list_index = lo_offset (label);
            if (result_centroid_ids)
                result_centroid_ids[i] = list_no;
            labels[i] = index_ivf->invlists->get_single_id(list_no, list_index);
        }
    }
}


SlidingIndexWindow::SlidingIndexWindow (Index *index): index (index) {
    n_slice = 0;
    IndexIVF* index_ivf = const_cast<IndexIVF*>(extract_index_ivf (index));
    ils = dynamic_cast<ArrayInvertedLists *> (index_ivf->invlists);
    nlist = ils->nlist;
    FAISS_THROW_IF_NOT_MSG (ils,
               "only supports indexes with ArrayInvertedLists");
    sizes.resize(nlist);
}

template<class T>
static void shift_and_add (std::vector<T> & dst,
                           size_t remove,
                           const std::vector<T> & src)
{
    if (remove > 0)
        memmove (dst.data(), dst.data() + remove,
                 (dst.size() - remove) * sizeof (T));
    size_t insert_point = dst.size() - remove;
    dst.resize (insert_point + src.size());
    memcpy (dst.data() + insert_point, src.data (), src.size() * sizeof(T));
}

template<class T>
static void remove_from_begin (std::vector<T> & v,
                               size_t remove)
{
    if (remove > 0)
        v.erase (v.begin(), v.begin() + remove);
}

void SlidingIndexWindow::step(const Index *sub_index, bool remove_oldest) {

    FAISS_THROW_IF_NOT_MSG (!remove_oldest || n_slice > 0,
                            "cannot remove slice: there is none");

    const ArrayInvertedLists *ils2 = nullptr;
    if(sub_index) {
        check_compatible_for_merge (index, sub_index);
        ils2 = dynamic_cast<const ArrayInvertedLists*>(
                                   extract_index_ivf (sub_index)->invlists);
        FAISS_THROW_IF_NOT_MSG (ils2, "supports only ArrayInvertedLists");
    }
    IndexIVF *index_ivf = extract_index_ivf (index);

    if (remove_oldest && ils2) {
        for (int i = 0; i < nlist; i++) {
            std::vector<size_t> & sizesi = sizes[i];
            size_t amount_to_remove = sizesi[0];
            index_ivf->ntotal += ils2->ids[i].size() - amount_to_remove;

            shift_and_add (ils->ids[i], amount_to_remove, ils2->ids[i]);
            shift_and_add (ils->codes[i], amount_to_remove * ils->code_size,
                           ils2->codes[i]);
            for (int j = 0; j + 1 < n_slice; j++) {
                sizesi[j] = sizesi[j + 1] - amount_to_remove;
            }
            sizesi[n_slice - 1] = ils->ids[i].size();
        }
    } else if (ils2) {
        for (int i = 0; i < nlist; i++) {
            index_ivf->ntotal += ils2->ids[i].size();
            shift_and_add (ils->ids[i], 0, ils2->ids[i]);
            shift_and_add (ils->codes[i], 0, ils2->codes[i]);
            sizes[i].push_back(ils->ids[i].size());
        }
        n_slice++;
    } else if (remove_oldest) {
        for (int i = 0; i < nlist; i++) {
            size_t amount_to_remove = sizes[i][0];
            index_ivf->ntotal -= amount_to_remove;
            remove_from_begin (ils->ids[i], amount_to_remove);
            remove_from_begin (ils->codes[i],
                               amount_to_remove * ils->code_size);
            for (int j = 0; j + 1 < n_slice; j++) {
                sizes[i][j] = sizes[i][j + 1] - amount_to_remove;
            }
            sizes[i].pop_back ();
        }
        n_slice--;
    } else {
        FAISS_THROW_MSG ("nothing to do???");
    }
    index->ntotal = index_ivf->ntotal;
}



// Get a subset of inverted lists [i0, i1). Works on IndexIVF's and
// IndexIVF's embedded in a IndexPreTransform

ArrayInvertedLists *
get_invlist_range (const Index *index, long i0, long i1)
{
    const IndexIVF *ivf = extract_index_ivf (index);

    FAISS_THROW_IF_NOT (0 <= i0 && i0 <= i1 && i1 <= ivf->nlist);

    const InvertedLists *src = ivf->invlists;

    ArrayInvertedLists * il = new ArrayInvertedLists(i1 - i0, src->code_size);

    for (long i = i0; i < i1; i++) {
        il->add_entries(i - i0, src->list_size(i),
                        InvertedLists::ScopedIds (src, i).get(),
                        InvertedLists::ScopedCodes (src, i).get());
    }
    return il;
}



void set_invlist_range (Index *index, long i0, long i1,
                        ArrayInvertedLists * src)
{
    IndexIVF *ivf = extract_index_ivf (index);

    FAISS_THROW_IF_NOT (0 <= i0 && i0 <= i1 && i1 <= ivf->nlist);

    ArrayInvertedLists *dst = dynamic_cast<ArrayInvertedLists *>(ivf->invlists);
    FAISS_THROW_IF_NOT_MSG (dst, "only ArrayInvertedLists supported");
    FAISS_THROW_IF_NOT (src->nlist == i1 - i0 &&
                        dst->code_size == src->code_size);

    size_t ntotal = index->ntotal;
    for (long i = i0 ; i < i1; i++) {
        ntotal -= dst->list_size (i);
        ntotal += src->list_size (i - i0);
        std::swap (src->codes[i - i0], dst->codes[i]);
        std::swap (src->ids[i - i0], dst->ids[i]);
    }
    ivf->ntotal = index->ntotal = ntotal;
}


void search_with_parameters (const Index *index,
                             idx_t n, const float *x, idx_t k,
                             float *distances, idx_t *labels,
                             IVFSearchParameters *params,
                             size_t *nb_dis_ptr)
{
    FAISS_THROW_IF_NOT (params);
    const float *prev_x = x;
    ScopeDeleter<float> del;

    if (auto ip = dynamic_cast<const IndexPreTransform *> (index)) {
        x = ip->apply_chain (n, x);
        if (x != prev_x) {
            del.set(x);
        }
        index = ip->index;
    }

    std::vector<idx_t> Iq(params->nprobe * n);
    std::vector<float> Dq(params->nprobe * n);

    const IndexIVF *index_ivf = dynamic_cast<const IndexIVF *>(index);
    FAISS_THROW_IF_NOT (index_ivf);

    index_ivf->quantizer->search(n, x, params->nprobe,
                                 Dq.data(), Iq.data());

    if (nb_dis_ptr) {
        size_t nb_dis = 0;
        const InvertedLists *il = index_ivf->invlists;
        for (idx_t i = 0; i < n * params->nprobe; i++) {
          if (Iq[i] >= 0) {
              nb_dis += il->list_size(Iq[i]);
          }
        }
        *nb_dis_ptr = nb_dis;
    }

    index_ivf->search_preassigned(n, x, k, Iq.data(), Dq.data(),
                                  distances, labels,
                                  false, params);
}



} } // namespace faiss::ivflib
