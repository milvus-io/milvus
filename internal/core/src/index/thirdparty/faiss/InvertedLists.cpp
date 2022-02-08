/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/InvertedLists.h>

#include <cstdio>
#include <numeric>

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>

//TODO: refactor to decouple dependency between CPU and Cuda, or upgrade faiss
#ifndef USE_CPU
#include "gpu/utils/DeviceUtils.h"
#include "cuda.h"
#include "cuda_runtime.h"

namespace faiss {

/*
 * Use pin memory to build Readonly Inverted list will accelerate cuda memory copy, but it will downgrade cpu ivf search
 * performance. read only inverted list structure will also make ivf search performance not stable. ISSUE 500 mention
 * this problem. Best performance is the original inverted list with non pin memory.
 */

PageLockMemory::PageLockMemory(size_t size) : nbytes(size) {
    auto err = cudaHostAlloc(&(this->data), size, 0);
    if (err) {
        std::string msg =
            "Fail to alloc page lock memory " + std::to_string(size) + ", err code " + std::to_string((int32_t)err);
        FAISS_THROW_MSG(msg);
    }
}

PageLockMemory::~PageLockMemory() {
    CUDA_VERIFY(cudaFreeHost((void*)(this->data)));
}

PageLockMemory::PageLockMemory(const PageLockMemory& other) {
    auto err = cudaHostAlloc(&(this->data), other.nbytes, 0);
    if (err) {
        std::string msg = "Fail to alloc page lock memory " + std::to_string(other.nbytes) + ", err code " +
                          std::to_string((int32_t)err);
        FAISS_THROW_MSG(msg);
    }
    memcpy(this->data, other.data, other.nbytes);
    this->nbytes = other.nbytes;
}

PageLockMemory::PageLockMemory(PageLockMemory &&other) {
    this->data = other.data;
    this->nbytes = other.nbytes;
    other.data = nullptr;
    other.nbytes = 0;
}

}
#endif

namespace faiss {



/*****************************************
 * InvertedLists implementation
 ******************************************/

InvertedLists::InvertedLists (size_t nlist, size_t code_size):
    nlist (nlist), code_size (code_size)
{
}

InvertedLists::~InvertedLists ()
{}

InvertedLists::idx_t InvertedLists::get_single_id (
     size_t list_no, size_t offset) const
{
    assert (offset < list_size (list_no));
    return get_ids(list_no)[offset];
}


void InvertedLists::release_codes (size_t, const uint8_t *) const
{}

void InvertedLists::release_ids (size_t, const idx_t *) const
{}

void InvertedLists::prefetch_lists (const idx_t *, int) const
{}

const uint8_t * InvertedLists::get_single_code (
                   size_t list_no, size_t offset) const
{
    assert (offset < list_size (list_no));
    return get_codes(list_no) + offset * code_size;
}

size_t InvertedLists::add_entry (size_t list_no, idx_t theid,
                                 const uint8_t *code)
{
    return add_entries (list_no, 1, &theid, code);
}

size_t InvertedLists::add_entry_without_codes (size_t list_no, idx_t theid) 
{
    return add_entries_without_codes (list_no, 1, &theid);
}

size_t InvertedLists::add_entries_without_codes (size_t list_no, size_t n_entry,
                                                 const idx_t* ids) 
{
    return 0;
}

void InvertedLists::update_entry (size_t list_no, size_t offset,
                                        idx_t id, const uint8_t *code)
{
    update_entries (list_no, offset, 1, &id, code);
}

InvertedLists* InvertedLists::to_readonly() {
    return nullptr;
}

InvertedLists* InvertedLists::to_readonly_without_codes() {
    return nullptr;
}

bool InvertedLists::is_readonly() const {
    return false;
}

void InvertedLists::reset () {
    for (size_t i = 0; i < nlist; i++) {
        resize (i, 0);
    }
}

void InvertedLists::merge_from (InvertedLists *oivf, size_t add_id) {

#pragma omp parallel for
    for (idx_t i = 0; i < nlist; i++) {
        size_t list_size = oivf->list_size (i);
        ScopedIds ids (oivf, i);
        if (add_id == 0) {
            add_entries (i, list_size, ids.get (),
                         ScopedCodes (oivf, i).get());
        } else {
            std::vector <idx_t> new_ids (list_size);

            for (size_t j = 0; j < list_size; j++) {
                new_ids [j] = ids[j] + add_id;
            }
            add_entries (i, list_size, new_ids.data(),
                                   ScopedCodes (oivf, i).get());
        }
        oivf->resize (i, 0);
    }
}

double InvertedLists::imbalance_factor () const {
    std::vector<int> hist(nlist);

    for (size_t i = 0; i < nlist; i++) {
        hist[i] = list_size(i);
    }

    return faiss::imbalance_factor(nlist, hist.data());
}

void InvertedLists::print_stats () const {
    std::vector<int> sizes(40);
    for (size_t i = 0; i < nlist; i++) {
        for (size_t j = 0; j < sizes.size(); j++) {
            if ((list_size(i) >> j) == 0) {
                sizes[j]++;
                break;
            }
        }
    }
    for (size_t i = 0; i < sizes.size(); i++) {
        if (sizes[i]) {
            printf("list size in < %d: %d instances\n", 1 << i, sizes[i]);
        }
    }
}

size_t InvertedLists::compute_ntotal () const {
    size_t tot = 0;
    for (size_t i = 0; i < nlist; i++) {
        tot += list_size(i);
    }
    return tot;
}

/*****************************************
 * ArrayInvertedLists implementation
 ******************************************/

ArrayInvertedLists::ArrayInvertedLists (size_t nlist, size_t code_size):
    InvertedLists (nlist, code_size)
{
    ids.resize (nlist);
    codes.resize (nlist);
}

size_t ArrayInvertedLists::add_entries (
           size_t list_no, size_t n_entry,
           const idx_t* ids_in, const uint8_t *code)
{
    if (n_entry == 0) return 0;
    assert (list_no < nlist);
    size_t o = ids [list_no].size();
    ids [list_no].resize (o + n_entry);
    memcpy (&ids[list_no][o], ids_in, sizeof (ids_in[0]) * n_entry);
    codes [list_no].resize ((o + n_entry) * code_size);
    memcpy (&codes[list_no][o * code_size], code, code_size * n_entry);
    return o;
}

size_t ArrayInvertedLists::add_entries_without_codes (
           size_t list_no, size_t n_entry,
           const idx_t* ids_in)
{
    if (n_entry == 0) return 0;
    assert (list_no < nlist);
    size_t o = ids [list_no].size();
    ids [list_no].resize (o + n_entry);
    memcpy (&ids[list_no][o], ids_in, sizeof (ids_in[0]) * n_entry);
    return o;
}

size_t ArrayInvertedLists::list_size(size_t list_no) const
{
    assert (list_no < nlist);
    return ids[list_no].size();
}

const uint8_t * ArrayInvertedLists::get_codes (size_t list_no) const
{
    assert (list_no < nlist);
    return codes[list_no].data();
}


const InvertedLists::idx_t * ArrayInvertedLists::get_ids (size_t list_no) const
{
    assert (list_no < nlist);
    return ids[list_no].data();
}

void ArrayInvertedLists::resize (size_t list_no, size_t new_size)
{
    ids[list_no].resize (new_size);
    codes[list_no].resize (new_size * code_size);
}

void ArrayInvertedLists::update_entries (
      size_t list_no, size_t offset, size_t n_entry,
      const idx_t *ids_in, const uint8_t *codes_in)
{
    assert (list_no < nlist);
    assert (n_entry + offset <= ids[list_no].size());
    memcpy (&ids[list_no][offset], ids_in, sizeof(ids_in[0]) * n_entry);
    memcpy (&codes[list_no][offset * code_size], codes_in, code_size * n_entry);
}

InvertedLists* ArrayInvertedLists::to_readonly() {
    ReadOnlyArrayInvertedLists* readonly = new ReadOnlyArrayInvertedLists(*this);
    return readonly;
}

InvertedLists* ArrayInvertedLists::to_readonly_without_codes() {
    ReadOnlyArrayInvertedLists* readonly = new ReadOnlyArrayInvertedLists(*this, true);
    return readonly;
}

ArrayInvertedLists::~ArrayInvertedLists ()
{}

/*****************************************************************
 * ReadOnlyArrayInvertedLists implementations
 *****************************************************************/

ReadOnlyArrayInvertedLists::ReadOnlyArrayInvertedLists(size_t nlist,
                                                       size_t code_size, const std::vector<size_t>& list_length)
        : InvertedLists (nlist, code_size),
          readonly_length(list_length) {
    valid = readonly_length.size() == nlist;
    if (!valid) {
        FAISS_THROW_MSG ("Invalid list_length");
        return;
    }
    auto total_size = std::accumulate(readonly_length.begin(), readonly_length.end(), 0);
    readonly_offset.reserve(nlist);

#ifdef USE_CPU
    readonly_codes.reserve(total_size * code_size);
    readonly_ids.reserve(total_size);
#endif

    size_t offset = 0;
    for (auto i=0; i<readonly_length.size(); ++i) {
        readonly_offset.emplace_back(offset);
        offset += readonly_length[i];
    }
}

ReadOnlyArrayInvertedLists::ReadOnlyArrayInvertedLists(const ArrayInvertedLists& other)
        : InvertedLists (other.nlist, other.code_size) {
    readonly_length.resize(nlist);
    readonly_offset.resize(nlist);
    size_t offset = 0;
    for (auto i = 0; i < other.ids.size(); i++) {
        auto& list_ids = other.ids[i];
        readonly_length[i] = list_ids.size();
        readonly_offset[i] = offset;
        offset += list_ids.size();
    }

#ifdef USE_CPU
    for (auto i = 0; i < other.ids.size(); i++) {
        auto& list_ids = other.ids[i];
        readonly_ids.insert(readonly_ids.end(), list_ids.begin(), list_ids.end());

        auto& list_codes = other.codes[i];
        readonly_codes.insert(readonly_codes.end(), list_codes.begin(), list_codes.end());
    }
#else
    size_t ids_size = offset * sizeof(idx_t);
    size_t codes_size = offset * (this->code_size) * sizeof(uint8_t);
    pin_readonly_codes = std::make_shared<PageLockMemory>(codes_size);
    pin_readonly_ids = std::make_shared<PageLockMemory>(ids_size);

    offset = 0;
    for (auto i = 0; i < other.ids.size(); i++) {
        auto& list_ids = other.ids[i];
        auto& list_codes = other.codes[i];

        uint8_t* ids_ptr = (uint8_t*)(pin_readonly_ids->data) + offset * sizeof(idx_t);
        memcpy(ids_ptr, list_ids.data(), list_ids.size() * sizeof(idx_t));

        uint8_t* codes_ptr = (uint8_t*)(pin_readonly_codes->data) + offset * (this->code_size) * sizeof(uint8_t);
        memcpy(codes_ptr, list_codes.data(), list_codes.size() * sizeof(uint8_t));

        offset += list_ids.size();
    }
#endif

    valid = true;
}

ReadOnlyArrayInvertedLists::ReadOnlyArrayInvertedLists(const ArrayInvertedLists& other, bool offset_only)
        : InvertedLists (other.nlist, other.code_size) {
    readonly_length.resize(nlist);
    readonly_offset.resize(nlist);
    size_t offset = 0;
    for (auto i = 0; i < other.ids.size(); i++) {
        auto& list_ids = other.ids[i];
        readonly_length[i] = list_ids.size();
        readonly_offset[i] = offset;
        offset += list_ids.size();
    }

#ifdef USE_CPU
    for (auto i = 0; i < other.ids.size(); i++) {
        auto& list_ids = other.ids[i];
        readonly_ids.insert(readonly_ids.end(), list_ids.begin(), list_ids.end());
    }
#else
    size_t ids_size = offset * sizeof(idx_t);
    size_t codes_size = offset * (this->code_size) * sizeof(uint8_t);
    pin_readonly_codes = std::make_shared<PageLockMemory>(codes_size);
    pin_readonly_ids = std::make_shared<PageLockMemory>(ids_size);

    offset = 0;
    for (auto i = 0; i < other.ids.size(); i++) {
        auto& list_ids = other.ids[i];

        uint8_t* ids_ptr = (uint8_t*)(pin_readonly_ids->data) + offset * sizeof(idx_t);
        memcpy(ids_ptr, list_ids.data(), list_ids.size() * sizeof(idx_t));

        offset += list_ids.size();
    }
#endif

    valid = true;
}


ReadOnlyArrayInvertedLists::~ReadOnlyArrayInvertedLists() {
}

bool
ReadOnlyArrayInvertedLists::is_valid() {
    return valid;
}

size_t ReadOnlyArrayInvertedLists::add_entries (
        size_t , size_t ,
        const idx_t* , const uint8_t *)
{
    FAISS_THROW_MSG ("not implemented");
}

size_t ReadOnlyArrayInvertedLists::add_entries_without_codes (
           size_t , size_t ,
           const idx_t*)
{
    FAISS_THROW_MSG ("not implemented");
}

void ReadOnlyArrayInvertedLists::update_entries (size_t, size_t , size_t ,
                                                 const idx_t *, const uint8_t *)
{
    FAISS_THROW_MSG ("not implemented");
}

void ReadOnlyArrayInvertedLists::resize (size_t , size_t )
{
    FAISS_THROW_MSG ("not implemented");
}

size_t ReadOnlyArrayInvertedLists::list_size(size_t list_no) const
{
    FAISS_ASSERT(list_no < nlist && valid);
    return readonly_length[list_no];
}

const uint8_t * ReadOnlyArrayInvertedLists::get_codes (size_t list_no) const
{
    FAISS_ASSERT(list_no < nlist && valid);
#ifdef USE_CPU
    return readonly_codes.data() + readonly_offset[list_no] * code_size;
#else
    uint8_t *pcodes = (uint8_t *)(pin_readonly_codes->data);
    return pcodes + readonly_offset[list_no] * code_size;
#endif
}

const InvertedLists::idx_t* ReadOnlyArrayInvertedLists::get_ids (size_t list_no) const
{
    FAISS_ASSERT(list_no < nlist && valid);
#ifdef USE_CPU
    return readonly_ids.data() + readonly_offset[list_no];
#else
    idx_t *pids = (idx_t *)pin_readonly_ids->data;
    return pids + readonly_offset[list_no];
#endif
}

const InvertedLists::idx_t* ReadOnlyArrayInvertedLists::get_all_ids() const {
    FAISS_ASSERT(valid);
#ifdef USE_CPU
    return readonly_ids.data();
#else
    return (idx_t *)(pin_readonly_ids->data);
#endif
}

const uint8_t* ReadOnlyArrayInvertedLists::get_all_codes() const {
    FAISS_ASSERT(valid);
#ifdef USE_CPU
    return readonly_codes.data();
#else
    return (uint8_t *)(pin_readonly_codes->data);
#endif
}

const std::vector<size_t>& ReadOnlyArrayInvertedLists::get_list_length() const {
    FAISS_ASSERT(valid);
    return readonly_length;
}

bool ReadOnlyArrayInvertedLists::is_readonly() const {
    FAISS_ASSERT(valid);
    return true;
}

/*****************************************************************
 * Meta-inverted list implementations
 *****************************************************************/


size_t ReadOnlyInvertedLists::add_entries (
           size_t , size_t ,
           const idx_t* , const uint8_t *)
{
    FAISS_THROW_MSG ("not implemented");
}

size_t ReadOnlyInvertedLists::add_entries_without_codes (
           size_t , size_t ,
           const idx_t*)
{
    FAISS_THROW_MSG ("not implemented");
}

void ReadOnlyInvertedLists::update_entries (size_t, size_t , size_t ,
                         const idx_t *, const uint8_t *)
{
    FAISS_THROW_MSG ("not implemented");
}

void ReadOnlyInvertedLists::resize (size_t , size_t )
{
    FAISS_THROW_MSG ("not implemented");
}



/*****************************************
 * HStackInvertedLists implementation
 ******************************************/

HStackInvertedLists::HStackInvertedLists (
          int nil, const InvertedLists **ils_in):
    ReadOnlyInvertedLists (nil > 0 ? ils_in[0]->nlist : 0,
                   nil > 0 ? ils_in[0]->code_size : 0)
{
    FAISS_THROW_IF_NOT (nil > 0);
    for (int i = 0; i < nil; i++) {
        ils.push_back (ils_in[i]);
        FAISS_THROW_IF_NOT (ils_in[i]->code_size == code_size &&
                            ils_in[i]->nlist == nlist);
    }
}

size_t HStackInvertedLists::list_size(size_t list_no) const
{
    size_t sz = 0;
    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        sz += il->list_size (list_no);
    }
    return sz;
}

const uint8_t * HStackInvertedLists::get_codes (size_t list_no) const
{
    uint8_t *codes = new uint8_t [code_size * list_size(list_no)], *c = codes;

    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        size_t sz = il->list_size(list_no) * code_size;
        if (sz > 0) {
            memcpy (c, ScopedCodes (il, list_no).get(), sz);
            c += sz;
        }
    }
    return codes;
}

const uint8_t * HStackInvertedLists::get_single_code (
           size_t list_no, size_t offset) const
{
    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        size_t sz = il->list_size (list_no);
        if (offset < sz) {
            // here we have to copy the code, otherwise it will crash at dealloc
            uint8_t * code = new uint8_t [code_size];
            memcpy (code, ScopedCodes (il, list_no, offset).get(), code_size);
            return code;
        }
        offset -= sz;
    }
    FAISS_THROW_FMT ("offset %ld unknown", offset);
}


void HStackInvertedLists::release_codes (size_t, const uint8_t *codes) const {
    delete [] codes;
}

const Index::idx_t * HStackInvertedLists::get_ids (size_t list_no) const
{
    idx_t *ids = new idx_t [list_size(list_no)], *c = ids;

    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        size_t sz = il->list_size(list_no);
        if (sz > 0) {
            memcpy (c, ScopedIds (il, list_no).get(), sz * sizeof(idx_t));
            c += sz;
        }
    }
    return ids;
}

Index::idx_t HStackInvertedLists::get_single_id (
                    size_t list_no, size_t offset) const
{

    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        size_t sz = il->list_size (list_no);
        if (offset < sz) {
            return il->get_single_id (list_no, offset);
        }
        offset -= sz;
    }
    FAISS_THROW_FMT ("offset %ld unknown", offset);
}


void HStackInvertedLists::release_ids (size_t, const idx_t *ids) const {
    delete [] ids;
}

void HStackInvertedLists::prefetch_lists (const idx_t *list_nos, int nlist) const
{
    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        il->prefetch_lists (list_nos, nlist);
    }
}

/*****************************************
 * SliceInvertedLists implementation
 ******************************************/


namespace {

    using idx_t = InvertedLists::idx_t;

    idx_t translate_list_no (const SliceInvertedLists *sil,
                             idx_t list_no) {
        FAISS_THROW_IF_NOT (list_no >= 0 && list_no < sil->nlist);
        return list_no + sil->i0;
    }

};



SliceInvertedLists::SliceInvertedLists (
    const InvertedLists *il, idx_t i0, idx_t i1):
    ReadOnlyInvertedLists (i1 - i0, il->code_size),
    il (il), i0(i0), i1(i1)
{

}

size_t SliceInvertedLists::list_size(size_t list_no) const
{
    return il->list_size (translate_list_no (this, list_no));
}

const uint8_t * SliceInvertedLists::get_codes (size_t list_no) const
{
    return il->get_codes (translate_list_no (this, list_no));
}

const uint8_t * SliceInvertedLists::get_single_code (
           size_t list_no, size_t offset) const
{
    return il->get_single_code (translate_list_no (this, list_no), offset);
}


void SliceInvertedLists::release_codes (
       size_t list_no, const uint8_t *codes) const {
    return il->release_codes (translate_list_no (this, list_no), codes);
}

const Index::idx_t * SliceInvertedLists::get_ids (size_t list_no) const
{
    return il->get_ids (translate_list_no (this, list_no));
}

Index::idx_t SliceInvertedLists::get_single_id (
                    size_t list_no, size_t offset) const
{
    return il->get_single_id (translate_list_no (this, list_no), offset);
}


void SliceInvertedLists::release_ids (size_t list_no, const idx_t *ids) const {
    return il->release_ids (translate_list_no (this, list_no), ids);
}

void SliceInvertedLists::prefetch_lists (const idx_t *list_nos, int nlist) const
{
    std::vector<idx_t> translated_list_nos;
    for (int j = 0; j < nlist; j++) {
        idx_t list_no = list_nos[j];
        if (list_no < 0) continue;
        translated_list_nos.push_back (translate_list_no (this, list_no));
    }
    il->prefetch_lists (translated_list_nos.data(),
                        translated_list_nos.size());
}


/*****************************************
 * VStackInvertedLists implementation
 ******************************************/

namespace {

    using idx_t = InvertedLists::idx_t;

    // find the invlist this number belongs to
    int translate_list_no (const VStackInvertedLists *vil,
                             idx_t list_no) {
        FAISS_THROW_IF_NOT (list_no >= 0 && list_no < vil->nlist);
        int i0 = 0, i1 = vil->ils.size();
        const idx_t *cumsz = vil->cumsz.data();
        while (i0 + 1 < i1) {
            int imed = (i0 + i1) / 2;
            if (list_no >= cumsz[imed]) {
                i0 = imed;
            } else {
                i1 = imed;
            }
        }
        assert(list_no >= cumsz[i0] && list_no < cumsz[i0 + 1]);
        return i0;
    }

    idx_t sum_il_sizes (int nil, const InvertedLists **ils_in) {
        idx_t tot = 0;
        for (int i = 0; i < nil; i++) {
            tot += ils_in[i]->nlist;
        }
        return tot;
    }

};



VStackInvertedLists::VStackInvertedLists (
          int nil, const InvertedLists **ils_in):
    ReadOnlyInvertedLists (sum_il_sizes(nil, ils_in),
                   nil > 0 ? ils_in[0]->code_size : 0)
{
    FAISS_THROW_IF_NOT (nil > 0);
    cumsz.resize (nil + 1);
    for (int i = 0; i < nil; i++) {
        ils.push_back (ils_in[i]);
        FAISS_THROW_IF_NOT (ils_in[i]->code_size == code_size);
        cumsz[i + 1] = cumsz[i] + ils_in[i]->nlist;
    }
}

size_t VStackInvertedLists::list_size(size_t list_no) const
{
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->list_size (list_no);
}

const uint8_t * VStackInvertedLists::get_codes (size_t list_no) const
{
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->get_codes (list_no);
}

const uint8_t * VStackInvertedLists::get_single_code (
           size_t list_no, size_t offset) const
{
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->get_single_code (list_no, offset);
}


void VStackInvertedLists::release_codes (
          size_t list_no, const uint8_t *codes) const {
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->release_codes (list_no, codes);
}

const Index::idx_t * VStackInvertedLists::get_ids (size_t list_no) const
{
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->get_ids (list_no);
}

Index::idx_t VStackInvertedLists::get_single_id (
                    size_t list_no, size_t offset) const
{
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->get_single_id (list_no, offset);
}


void VStackInvertedLists::release_ids (size_t list_no, const idx_t *ids) const {
    int i = translate_list_no (this, list_no);
    list_no -= cumsz[i];
    return ils[i]->release_ids (list_no, ids);
}

void VStackInvertedLists::prefetch_lists (
           const idx_t *list_nos, int nlist) const
{
    std::vector<int> ilno (nlist, -1);
    std::vector<int> n_per_il (ils.size(), 0);
    for (int j = 0; j < nlist; j++) {
        idx_t list_no = list_nos[j];
        if (list_no < 0) continue;
        int i = ilno[j] = translate_list_no (this, list_no);
        n_per_il[i]++;
    }
    std::vector<int> cum_n_per_il (ils.size() + 1, 0);
    for (int j = 0; j < ils.size(); j++) {
        cum_n_per_il[j + 1] = cum_n_per_il[j] + n_per_il[j];
    }
    std::vector<idx_t> sorted_list_nos (cum_n_per_il.back());
    for (int j = 0; j < nlist; j++) {
        idx_t list_no = list_nos[j];
        if (list_no < 0) continue;
        int i = ilno[j];
        list_no -= cumsz[i];
        sorted_list_nos[cum_n_per_il[i]++] = list_no;
    }

    int i0 = 0;
    for (int j = 0; j < ils.size(); j++) {
        int i1 = i0 + n_per_il[j];
        if (i1 > i0) {
            ils[j]->prefetch_lists (sorted_list_nos.data() + i0,
                                    i1 - i0);
        }
        i0 = i1;
    }
}



/*****************************************
 * MaskedInvertedLists implementation
 ******************************************/


MaskedInvertedLists::MaskedInvertedLists (const InvertedLists *il0,
                                          const InvertedLists *il1):
    ReadOnlyInvertedLists (il0->nlist, il0->code_size),
    il0 (il0), il1 (il1)
{
    FAISS_THROW_IF_NOT (il1->nlist == nlist);
    FAISS_THROW_IF_NOT (il1->code_size == code_size);
}

size_t MaskedInvertedLists::list_size(size_t list_no) const
{
    size_t sz = il0->list_size(list_no);
    return sz ? sz : il1->list_size(list_no);
}

const uint8_t * MaskedInvertedLists::get_codes (size_t list_no) const
{
    size_t sz = il0->list_size(list_no);
    return (sz ? il0 : il1)->get_codes(list_no);
}

const idx_t * MaskedInvertedLists::get_ids (size_t list_no) const
{
    size_t sz = il0->list_size (list_no);
    return (sz ? il0 : il1)->get_ids (list_no);
}

void MaskedInvertedLists::release_codes (
      size_t list_no, const uint8_t *codes) const
{
    size_t sz = il0->list_size (list_no);
    (sz ? il0 : il1)->release_codes (list_no, codes);
}

void MaskedInvertedLists::release_ids (size_t list_no, const idx_t *ids) const
{
    size_t sz = il0->list_size (list_no);
    (sz ? il0 : il1)->release_ids (list_no, ids);
}

idx_t MaskedInvertedLists::get_single_id (size_t list_no, size_t offset) const
{
    size_t sz = il0->list_size (list_no);
    return (sz ? il0 : il1)->get_single_id (list_no, offset);
}

const uint8_t * MaskedInvertedLists::get_single_code (
           size_t list_no, size_t offset) const
{
    size_t sz = il0->list_size (list_no);
    return (sz ? il0 : il1)->get_single_code (list_no, offset);
}

void MaskedInvertedLists::prefetch_lists (
       const idx_t *list_nos, int nlist) const
{
    std::vector<idx_t> list0, list1;
    for (int i = 0; i < nlist; i++) {
        idx_t list_no = list_nos[i];
        if (list_no < 0) continue;
        size_t sz = il0->list_size(list_no);
        (sz ? list0 : list1).push_back (list_no);
    }
    il0->prefetch_lists (list0.data(), list0.size());
    il1->prefetch_lists (list1.data(), list1.size());
}



} // namespace faiss
