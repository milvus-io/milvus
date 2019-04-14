/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "InvertedLists.h"

#include <cstdio>

#include "utils.h"
#include "FaissAssert.h"

namespace faiss {

using ScopedIds = InvertedLists::ScopedIds;
using ScopedCodes = InvertedLists::ScopedCodes;


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


void InvertedLists::release_codes (const uint8_t *) const
{}

void InvertedLists::release_ids (const idx_t *) const
{}

void InvertedLists::prefetch_lists (const long *, int) const
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

void InvertedLists::update_entry (size_t list_no, size_t offset,
                                        idx_t id, const uint8_t *code)
{
    update_entries (list_no, offset, 1, &id, code);
}

void InvertedLists::reset () {
    for (size_t i = 0; i < nlist; i++) {
        resize (i, 0);
    }
}

void InvertedLists::merge_from (InvertedLists *oivf, size_t add_id) {

#pragma omp parallel for
    for (long i = 0; i < nlist; i++) {
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


ArrayInvertedLists::~ArrayInvertedLists ()
{}


/*****************************************
 * ConcatenatedInvertedLists implementation
 ******************************************/

ConcatenatedInvertedLists::ConcatenatedInvertedLists (
          int nil, const InvertedLists **ils_in):
    InvertedLists (nil > 0 ? ils_in[0]->nlist : 0,
                   nil > 0 ? ils_in[0]->code_size : 0)
{
    FAISS_THROW_IF_NOT (nil > 0);
    for (int i = 0; i < nil; i++) {
        ils.push_back (ils_in[i]);
        FAISS_THROW_IF_NOT (ils_in[i]->code_size == code_size &&
                            ils_in[i]->nlist == nlist);
    }
}

size_t ConcatenatedInvertedLists::list_size(size_t list_no) const
{
    size_t sz = 0;
    for (int i = 0; i < ils.size(); i++) {
        const InvertedLists *il = ils[i];
        sz += il->list_size (list_no);
    }
    return sz;
}

const uint8_t * ConcatenatedInvertedLists::get_codes (size_t list_no) const
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

const uint8_t * ConcatenatedInvertedLists::get_single_code (
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


void ConcatenatedInvertedLists::release_codes (const uint8_t *codes) const {
    delete [] codes;
}

const Index::idx_t * ConcatenatedInvertedLists::get_ids (size_t list_no) const
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

Index::idx_t ConcatenatedInvertedLists::get_single_id (
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


void ConcatenatedInvertedLists::release_ids (const idx_t *ids) const {
    delete [] ids;
}

size_t ConcatenatedInvertedLists::add_entries (
           size_t , size_t ,
           const idx_t* , const uint8_t *)
{
    FAISS_THROW_MSG ("not implemented");
}

void ConcatenatedInvertedLists::update_entries (size_t, size_t , size_t ,
                         const idx_t *, const uint8_t *)
{
    FAISS_THROW_MSG ("not implemented");
}

void ConcatenatedInvertedLists::resize (size_t , size_t )
{
    FAISS_THROW_MSG ("not implemented");
}




} // namespace faiss
