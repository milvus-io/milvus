/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/GpuCloner.h>
#include <typeinfo>

#include <faiss/gpu/GpuIndex.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/index_io.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexScalarQuantizer.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexReplicas.h>
#include <faiss/IndexPreTransform.h>
#include <faiss/MetaIndexes.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/GpuIndexIVFScalarQuantizer.h>
#include <faiss/gpu/utils/DeviceUtils.h>

namespace faiss { namespace gpu {


/**********************************************************
 * Cloning to CPU
 **********************************************************/

void ToCPUCloner::merge_index(Index *dst, Index *src, bool successive_ids)
{
    if (auto ifl = dynamic_cast<IndexFlat *>(dst)) {
        auto ifl2 = dynamic_cast<const IndexFlat *>(src);
        FAISS_ASSERT(ifl2);
        FAISS_ASSERT(successive_ids);
        ifl->add(ifl2->ntotal, ifl2->xb.data());
    } else if(auto ifl = dynamic_cast<IndexIVFFlat *>(dst)) {
        auto ifl2 = dynamic_cast<IndexIVFFlat *>(src);
        FAISS_ASSERT(ifl2);
        ifl->merge_from(*ifl2, successive_ids ? ifl->ntotal : 0);
    } else if(auto ifl = dynamic_cast<IndexIVFScalarQuantizer *>(dst)) {
        auto ifl2 = dynamic_cast<IndexIVFScalarQuantizer *>(src);
        FAISS_ASSERT(ifl2);
        ifl->merge_from(*ifl2, successive_ids ? ifl->ntotal : 0);
    } else if(auto ifl = dynamic_cast<IndexIVFPQ *>(dst)) {
        auto ifl2 = dynamic_cast<IndexIVFPQ *>(src);
        FAISS_ASSERT(ifl2);
        ifl->merge_from(*ifl2, successive_ids ? ifl->ntotal : 0);
    } else {
        FAISS_ASSERT(!"merging not implemented for this type of class");
    }
}


Index *ToCPUCloner::clone_Index(const Index *index)
{
    if(auto ifl = dynamic_cast<const GpuIndexFlat *>(index)) {
        IndexFlat *res = new IndexFlat();
        ifl->copyTo(res);
        return res;
    } else if(auto ifl = dynamic_cast<const GpuIndexIVFFlat *>(index)) {
        IndexIVFFlat *res = new IndexIVFFlat();
        ifl->copyTo(res);
        return res;
    } else if(auto ifl =
              dynamic_cast<const GpuIndexIVFScalarQuantizer *>(index)) {
        IndexIVFScalarQuantizer *res = new IndexIVFScalarQuantizer();
        ifl->copyTo(res);
        return res;
    } else if(auto ipq = dynamic_cast<const GpuIndexIVFPQ *>(index)) {
        IndexIVFPQ *res = new IndexIVFPQ();
        ipq->copyTo(res);
        return res;

        // for IndexShards and IndexReplicas we assume that the
        // objective is to make a single component out of them
        // (inverse op of ToGpuClonerMultiple)

    } else if(auto ish = dynamic_cast<const IndexShards *>(index)) {
        int nshard = ish->count();
        FAISS_ASSERT(nshard > 0);
        Index *res = clone_Index(ish->at(0));
        for(int i = 1; i < ish->count(); i++) {
            Index *res_i = clone_Index(ish->at(i));
            merge_index(res, res_i, ish->successive_ids);
            delete res_i;
        }
        return res;
    } else if(auto ipr = dynamic_cast<const IndexReplicas *>(index)) {
        // just clone one of the replicas
        FAISS_ASSERT(ipr->count() > 0);
        return clone_Index(ipr->at(0));
    } else {
        return Cloner::clone_Index(index);
    }
}

faiss::Index * index_gpu_to_cpu(const faiss::Index *gpu_index)
{
    ToCPUCloner cl;
    return cl.clone_Index(gpu_index);
}




/**********************************************************
 * Cloning to 1 GPU
 **********************************************************/

ToGpuCloner::ToGpuCloner(GpuResources *resources, int device,
                         const GpuClonerOptions &options):
    GpuClonerOptions(options), resources(resources), device(device)
{}

Index *ToGpuCloner::clone_Index(const Index *index)
{
    if(auto ifl = dynamic_cast<const IndexFlat *>(index)) {
        GpuIndexFlatConfig config;
        config.device = device;
        config.useFloat16 = useFloat16;
        config.storeTransposed = storeTransposed;

        return new GpuIndexFlat(resources, ifl, config);
    } else if(auto ifl = dynamic_cast<const faiss::IndexIVFFlat *>(index)) {
        GpuIndexIVFFlatConfig config;
        config.device = device;
        config.indicesOptions = indicesOptions;
        config.flatConfig.useFloat16 = useFloat16CoarseQuantizer;
        config.flatConfig.storeTransposed = storeTransposed;

        GpuIndexIVFFlat *res =
            new GpuIndexIVFFlat(resources,
                                ifl->d,
                                ifl->nlist,
                                ifl->metric_type,
                                config);
        if(reserveVecs > 0 && ifl->ntotal == 0) {
            res->reserveMemory(reserveVecs);
        }

        res->copyFrom(ifl);
        return res;
    } else if(auto ifl =
              dynamic_cast<const faiss::IndexIVFScalarQuantizer *>(index)) {
        GpuIndexIVFScalarQuantizerConfig config;
        config.device = device;
        config.indicesOptions = indicesOptions;
        config.flatConfig.useFloat16 = useFloat16CoarseQuantizer;
        config.flatConfig.storeTransposed = storeTransposed;

        GpuIndexIVFScalarQuantizer *res =
            new GpuIndexIVFScalarQuantizer(resources,
                                           ifl->d,
                                           ifl->nlist,
                                           ifl->sq.qtype,
                                           ifl->metric_type,
                                           ifl->by_residual,
                                           config);
        if(reserveVecs > 0 && ifl->ntotal == 0) {
            res->reserveMemory(reserveVecs);
        }

        res->copyFrom(ifl);
        return res;
    } else if(auto ipq = dynamic_cast<const faiss::IndexIVFPQ *>(index)) {
        if(verbose)
            printf("  IndexIVFPQ size %ld -> GpuIndexIVFPQ "
                   "indicesOptions=%d "
                   "usePrecomputed=%d useFloat16=%d reserveVecs=%ld\n",
                   ipq->ntotal, indicesOptions, usePrecomputed,
                   useFloat16, reserveVecs);
        GpuIndexIVFPQConfig config;
        config.device = device;
        config.indicesOptions = indicesOptions;
        config.flatConfig.useFloat16 = useFloat16CoarseQuantizer;
        config.flatConfig.storeTransposed = storeTransposed;
        config.useFloat16LookupTables = useFloat16;
        config.usePrecomputedTables = usePrecomputed;

        GpuIndexIVFPQ *res = new GpuIndexIVFPQ(resources, ipq, config);

        if(reserveVecs > 0 && ipq->ntotal == 0) {
            res->reserveMemory(reserveVecs);
        }

        return res;
    } else {
        return Cloner::clone_Index(index);
    }
}


faiss::Index * index_cpu_to_gpu(
       GpuResources* resources, int device,
       const faiss::Index *index,
       const GpuClonerOptions *options)
{
    GpuClonerOptions defaults;
    ToGpuCloner cl(resources, device, options ? *options : defaults);
    return cl.clone_Index(index);
}


/**********************************************************
 * Cloning to multiple GPUs
 **********************************************************/

ToGpuClonerMultiple::ToGpuClonerMultiple(
                        std::vector<GpuResources *> & resources,
                        std::vector<int>& devices,
                        const GpuMultipleClonerOptions &options):
    GpuMultipleClonerOptions(options)
{
    FAISS_ASSERT(resources.size() == devices.size());
    for(int i = 0; i < resources.size(); i++) {
        sub_cloners.push_back(ToGpuCloner(resources[i], devices[i], options));
    }
}


ToGpuClonerMultiple::ToGpuClonerMultiple(
                        const std::vector<ToGpuCloner> & sub_cloners,
                        const GpuMultipleClonerOptions &options):
    GpuMultipleClonerOptions(options),
    sub_cloners(sub_cloners)
{}


void ToGpuClonerMultiple::copy_ivf_shard (
                         const IndexIVF *index_ivf, IndexIVF *idx2,
                         long n, long i)
{
    if (shard_type == 2) {
        long i0 = i * index_ivf->ntotal / n;
        long i1 = (i + 1) * index_ivf->ntotal / n;

        if(verbose)
            printf("IndexShards shard %ld indices %ld:%ld\n",
                   i, i0, i1);
        index_ivf->copy_subset_to(*idx2, 2, i0, i1);
        FAISS_ASSERT(idx2->ntotal == i1 - i0);
    } else if (shard_type == 1) {
        if(verbose)
            printf("IndexShards shard %ld select modulo %ld = %ld\n",
                   i, n, i);
        index_ivf->copy_subset_to(*idx2, 1, n, i);
    } else {
        FAISS_THROW_FMT ("shard_type %d not implemented", shard_type);
    }

}

Index * ToGpuClonerMultiple::clone_Index_to_shards (const Index *index)
{
    long n = sub_cloners.size();

    auto index_ivfpq =
        dynamic_cast<const faiss::IndexIVFPQ *>(index);
    auto index_ivfflat =
        dynamic_cast<const faiss::IndexIVFFlat *>(index);
    auto index_ivfsq =
        dynamic_cast<const faiss::IndexIVFScalarQuantizer *>(index);
    auto index_flat =
        dynamic_cast<const faiss::IndexFlat *>(index);
    FAISS_THROW_IF_NOT_MSG (
        index_ivfpq || index_ivfflat || index_flat || index_ivfsq,
        "IndexShards implemented only for "
        "IndexIVFFlat, IndexIVFScalarQuantizer, "
        "IndexFlat and IndexIVFPQ");

    std::vector<faiss::Index*> shards(n);

    for(long i = 0; i < n; i++) {
        // make a shallow copy
        if(reserveVecs)
            sub_cloners[i].reserveVecs =
                (reserveVecs + n - 1) / n;

        if (index_ivfpq) {
            faiss::IndexIVFPQ idx2(
                       index_ivfpq->quantizer, index_ivfpq->d,
                       index_ivfpq->nlist, index_ivfpq->code_size,
                       index_ivfpq->pq.nbits);
            idx2.metric_type = index_ivfpq->metric_type;
            idx2.pq = index_ivfpq->pq;
            idx2.nprobe = index_ivfpq->nprobe;
            idx2.use_precomputed_table = 0;
            idx2.is_trained = index->is_trained;
            copy_ivf_shard (index_ivfpq, &idx2, n, i);
            shards[i] = sub_cloners[i].clone_Index(&idx2);
        } else if (index_ivfflat) {
            faiss::IndexIVFFlat idx2(
                       index_ivfflat->quantizer, index->d,
                       index_ivfflat->nlist, index_ivfflat->metric_type);
            idx2.nprobe = index_ivfflat->nprobe;
            idx2.is_trained = index->is_trained;
            copy_ivf_shard (index_ivfflat, &idx2, n, i);
            shards[i] = sub_cloners[i].clone_Index(&idx2);
        } else if (index_ivfsq) {
            faiss::IndexIVFScalarQuantizer idx2(
                       index_ivfsq->quantizer, index->d, index_ivfsq->nlist,
                       index_ivfsq->sq.qtype,
                       index_ivfsq->metric_type,
                       index_ivfsq->by_residual);

            idx2.nprobe = index_ivfsq->nprobe;
            idx2.is_trained = index->is_trained;
            idx2.sq = index_ivfsq->sq;
            copy_ivf_shard (index_ivfsq, &idx2, n, i);
            shards[i] = sub_cloners[i].clone_Index(&idx2);
        } else if (index_flat) {
            faiss::IndexFlat idx2 (
                                   index->d, index->metric_type);
            shards[i] = sub_cloners[i].clone_Index(&idx2);
            if (index->ntotal > 0) {
                long i0 = index->ntotal * i / n;
                long i1 = index->ntotal * (i + 1) / n;
                shards[i]->add (i1 - i0,
                                index_flat->xb.data() + i0 * index->d);
            }
        }
    }

    bool successive_ids = index_flat != nullptr;
    faiss::IndexShards *res =
        new faiss::IndexShards(index->d, true,
                               successive_ids);

    for (int i = 0; i < n; i++) {
        res->add_shard(shards[i]);
    }
    res->own_fields = true;
    FAISS_ASSERT(index->ntotal == res->ntotal);
    return res;
}

Index *ToGpuClonerMultiple::clone_Index(const Index *index)
{
    long n = sub_cloners.size();
    if (n == 1)
        return sub_cloners[0].clone_Index(index);

    if(dynamic_cast<const IndexFlat *>(index) ||
       dynamic_cast<const faiss::IndexIVFFlat *>(index) ||
       dynamic_cast<const faiss::IndexIVFScalarQuantizer *>(index) ||
       dynamic_cast<const faiss::IndexIVFPQ *>(index)) {
        if(!shard) {
            IndexReplicas * res = new IndexReplicas();
            for(auto & sub_cloner: sub_cloners) {
                res->addIndex(sub_cloner.clone_Index(index));
            }
            res->own_fields = true;
            return res;
        } else {
            return clone_Index_to_shards (index);
        }
    } else if(auto miq = dynamic_cast<const MultiIndexQuantizer *>(index)) {
        if (verbose) {
            printf("cloning MultiIndexQuantizer: "
                   "will be valid only for search k=1\n");
        }
        const ProductQuantizer & pq = miq->pq;
        IndexSplitVectors *splitv = new IndexSplitVectors(pq.d, true);
        splitv->own_fields = true;

        for (int m = 0; m < pq.M; m++) {
            // which GPU(s) will be assigned to this sub-quantizer

            long i0 = m * n / pq.M;
            long i1 = pq.M <= n ? (m + 1) * n / pq.M : i0 + 1;
            std::vector<ToGpuCloner> sub_cloners_2;
            sub_cloners_2.insert(
                                 sub_cloners_2.begin(), sub_cloners.begin() + i0,
                                 sub_cloners.begin() + i1);
            ToGpuClonerMultiple cm(sub_cloners_2, *this);
            IndexFlatL2 idxc (pq.dsub);
            idxc.add (pq.ksub, pq.centroids.data() + m * pq.d * pq.ksub);
            Index *idx2 = cm.clone_Index(&idxc);
            splitv->add_sub_index(idx2);
        }
        return splitv;
    } else {
        return Cloner::clone_Index(index);
    }
}



faiss::Index * index_cpu_to_gpu_multiple(
       std::vector<GpuResources*> & resources,
       std::vector<int> &devices,
       const faiss::Index *index,
       const GpuMultipleClonerOptions *options)
{
    GpuMultipleClonerOptions defaults;
    ToGpuClonerMultiple cl(resources, devices, options ? *options : defaults);
    return cl.clone_Index(index);
}

} } // namespace
