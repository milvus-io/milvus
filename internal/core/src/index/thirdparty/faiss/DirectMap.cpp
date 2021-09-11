/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/DirectMap.h>

#include <cstdio>
#include <cassert>

#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>

namespace faiss {

DirectMap::DirectMap(): type(NoMap)
{}

void DirectMap::set_type (Type new_type, const InvertedLists *invlists, size_t ntotal) {

    FAISS_THROW_IF_NOT (new_type == NoMap || new_type == Array ||
                        new_type == Hashtable);

    if (new_type == type) {
        // nothing to do
        return;
    }

    array.clear ();
    hashtable.clear ();
    type = new_type;

    if (new_type == NoMap) {
        return;
    } else if (new_type == Array) {
        array.resize (ntotal, -1);
    } else if (new_type == Hashtable) {
        hashtable.reserve (ntotal);
    }

    for (size_t key = 0; key < invlists->nlist; key++) {
        size_t list_size = invlists->list_size (key);
        InvertedLists::ScopedIds idlist (invlists, key);

        if (new_type == Array) {
            for (long ofs = 0; ofs < list_size; ofs++) {
                FAISS_THROW_IF_NOT_MSG (
                       0 <= idlist [ofs] && idlist[ofs] < ntotal,
                       "direct map supported only for seuquential ids");
                array [idlist [ofs]] = lo_build(key, ofs);
            }
        } else if (new_type == Hashtable) {
            for (long ofs = 0; ofs < list_size; ofs++) {
                hashtable [idlist [ofs]] = lo_build(key, ofs);
            }
        }
    }
}

void DirectMap::clear()
{
    array.clear ();
    hashtable.clear ();
}


DirectMap::idx_t DirectMap::get (idx_t key) const
{
    if (type == Array) {
        FAISS_THROW_IF_NOT_MSG (
             key >= 0 && key < array.size(), "invalid key"
        );
        idx_t lo = array[key];
        FAISS_THROW_IF_NOT_MSG(lo >= 0, "-1 entry in direct_map");
        return lo;
    } else if (type == Hashtable) {
        auto res = hashtable.find (key);
        FAISS_THROW_IF_NOT_MSG (res != hashtable.end(), "key not found");
        return res->second;
    } else {
        FAISS_THROW_MSG ("direct map not initialized");
    }
}



void DirectMap::add_single_id (idx_t id, idx_t list_no, size_t offset)
{
    if (type == NoMap) return;

    if (type == Array) {
        assert (id == array.size());
        if (list_no >= 0) {
            array.push_back (lo_build (list_no, offset));
        } else {
            array.push_back (-1);
        }
    } else if (type == Hashtable) {
        if (list_no >= 0) {
            hashtable[id] = lo_build (list_no, offset);
        }
    }

}

void DirectMap::check_can_add (const idx_t *ids) {
    if (type == Array && ids) {
        FAISS_THROW_MSG ("cannot have array direct map and add with ids");
    }
}

/********************* DirectMapAdd implementation */


DirectMapAdd::DirectMapAdd (DirectMap &direct_map, size_t n, const idx_t *xids):
    direct_map(direct_map), type(direct_map.type), n(n), xids(xids)
{
    if (type == DirectMap::Array)  {
        FAISS_THROW_IF_NOT (xids == nullptr);
        ntotal = direct_map.array.size();
        direct_map.array.resize (ntotal + n, -1);
    } else if (type == DirectMap::Hashtable) {
        // can't parallel update hashtable so use temp array
        all_ofs.resize (n, -1);
    }
}


void DirectMapAdd::add (size_t i, idx_t list_no, size_t ofs)
{
    if (type == DirectMap::Array) {
        direct_map.array [ntotal + i] = lo_build (list_no, ofs);
    } else if (type == DirectMap::Hashtable) {
        all_ofs [i] = lo_build (list_no, ofs);
    }
}

DirectMapAdd::~DirectMapAdd ()
{
    if (type == DirectMap::Hashtable) {
        for (int i = 0; i < n; i++) {
            idx_t id = xids ? xids[i] : ntotal + i;
            direct_map.hashtable [id] = all_ofs [i];
        }
    }
}

/********************************************************/

using ScopedCodes = InvertedLists::ScopedCodes;
using ScopedIds = InvertedLists::ScopedIds;


size_t DirectMap::remove_ids(const IDSelector& sel, InvertedLists *invlists)
{
    size_t nlist = invlists->nlist;
    std::vector<idx_t> toremove(nlist);

    size_t nremove = 0;

    if (type == NoMap) {
        // exhaustive scan of IVF
#pragma omp parallel for
        for (idx_t i = 0; i < nlist; i++) {
            idx_t l0 = invlists->list_size (i), l = l0, j = 0;
            ScopedIds idsi (invlists, i);
            while (j < l) {
                if (sel.is_member (idsi[j])) {
                    l--;
                    invlists->update_entry (
                        i, j,
                        invlists->get_single_id (i, l),
                        ScopedCodes (invlists, i, l).get()
                    );
                } else {
                    j++;
                }
            }
            toremove[i] = l0 - l;
        }
        // this will not run well in parallel on ondisk because of
        // possible shrinks
        for (idx_t i = 0; i < nlist; i++) {
            if (toremove[i] > 0) {
                nremove += toremove[i];
                invlists->resize(i, invlists->list_size(i) - toremove[i]);
            }
        }
    } else if (type == Hashtable) {
        const IDSelectorArray *sela =
            dynamic_cast<const IDSelectorArray*>(&sel);
        FAISS_THROW_IF_NOT_MSG (
             sela,
             "remove with hashtable works only with IDSelectorArray"
        );

        for (idx_t i = 0; i < sela->n; i++) {
            idx_t id = sela->ids[i];
            auto res = hashtable.find (id);
            if (res != hashtable.end()) {
                size_t list_no = lo_listno (res->second);
                size_t offset = lo_offset (res->second);
                idx_t last = invlists->list_size (list_no) - 1;
                hashtable.erase (res);
                if (offset < last) {
                    idx_t last_id = invlists->get_single_id (list_no, last);
                    invlists->update_entry (
                        list_no, offset,
                        last_id,
                        ScopedCodes (invlists, list_no, last).get()
                    );
                    // update hash entry for last element
                    hashtable [last_id] = list_no << 32 | offset;
                }
                invlists->resize(list_no, last);
                nremove++;
            }
        }

    } else {
        FAISS_THROW_MSG("remove not supported with this direct_map format");
    }
    return nremove;
}

void DirectMap::update_codes (InvertedLists *invlists,
                              int n, const idx_t *ids,
                              const idx_t *assign,
                              const uint8_t *codes)
{
    FAISS_THROW_IF_NOT (type == Array);

    size_t code_size = invlists->code_size;

    for (size_t i = 0; i < n; i++) {
        idx_t id = ids[i];
        FAISS_THROW_IF_NOT_MSG (0 <= id && id < array.size(),
                                "id to update out of range");
        { // remove old one
            idx_t dm = array [id];
            int64_t ofs = lo_offset (dm);
            int64_t il = lo_listno (dm);
            size_t l = invlists->list_size (il);
            if (ofs != l - 1) { // move l - 1 to ofs
                int64_t id2 = invlists->get_single_id (il, l - 1);
                array[id2] = lo_build (il, ofs);
                invlists->update_entry (il, ofs, id2,
                                        invlists->get_single_code (il, l - 1));
            }
            invlists->resize (il, l - 1);
        }
        { // insert new one
            int64_t il = assign[i];
            size_t l = invlists->list_size (il);
            idx_t dm = lo_build (il, l);
            array [id] = dm;
            invlists->add_entry (il, id, codes + i * code_size);
        }
    }
}


}
