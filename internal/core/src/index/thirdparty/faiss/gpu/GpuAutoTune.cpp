/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/gpu/GpuAutoTune.h>
#include <typeinfo>

#include <faiss/gpu/GpuIndex.h>
#include <faiss/IndexReplicas.h>
#include <faiss/IndexShards.h>
#include <faiss/IndexPreTransform.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/GpuIndexIVFScalarQuantizer.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/gpu/utils/DeviceUtils.h>

namespace faiss { namespace gpu {


using namespace ::faiss;

/**********************************************************
 * Parameters to auto-tune on GpuIndex'es
 **********************************************************/

#define DC(classname) auto ix = dynamic_cast<const classname *>(index)


void GpuParameterSpace::initialize (const Index * index)
{
    if (DC (IndexPreTransform)) {
        index = ix->index;
    }
    if (DC (IndexReplicas)) {
        if (ix->count() == 0) return;
        index = ix->at(0);
    }
    if (DC (IndexShards)) {
        if (ix->count() == 0) return;
        index = ix->at(0);
    }
    if (DC (GpuIndexIVF)) {
        ParameterRange & pr = add_range("nprobe");
        for (int i = 0; i < 12; i++) {
            size_t nprobe = 1 << i;
            if (nprobe >= ix->getNumLists() ||
                nprobe > getMaxKSelection()) break;
            pr.values.push_back (nprobe);
        }
    }
    // not sure we should call the parent initializer
}



#undef DC
// non-const version
#define DC(classname) auto *ix = dynamic_cast<classname *>(index)



void GpuParameterSpace::set_index_parameter (
        Index * index, const std::string & name, double val) const
{
    if (DC (IndexReplicas)) {
        for (int i = 0; i < ix->count(); i++)
            set_index_parameter (ix->at(i), name, val);
        return;
    }
    if (name == "nprobe") {
        if (DC (GpuIndexIVF)) {
            ix->setNumProbes (int (val));
            return;
        }
    }
    if (name == "use_precomputed_table") {
        if (DC (GpuIndexIVFPQ)) {
            ix->setPrecomputedCodes(bool (val));
            return;
        }
    }

    // maybe normal index parameters apply?
    ParameterSpace::set_index_parameter (index, name, val);
}




} } // namespace
