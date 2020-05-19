/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/IVFPQ.cuh>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/impl/BroadcastSum.cuh>
#include <faiss/gpu/impl/Distance.cuh>
#include <faiss/gpu/impl/FlatIndex.cuh>
#include <faiss/gpu/impl/IVFAppend.cuh>
#include <faiss/gpu/impl/L2Norm.cuh>
#include <faiss/gpu/impl/PQCodeDistances.cuh>
#include <faiss/gpu/impl/PQScanMultiPassNoPrecomputed.cuh>
#include <faiss/gpu/impl/PQScanMultiPassPrecomputed.cuh>
#include <faiss/gpu/impl/RemapIndices.h>
#include <faiss/gpu/impl/VectorResidual.cuh>
#include <faiss/gpu/utils/ConversionOperators.cuh>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/HostTensor.cuh>
#include <faiss/gpu/utils/MatrixMult.cuh>
#include <faiss/gpu/utils/NoTypeTensor.cuh>
#include <faiss/gpu/utils/Transpose.cuh>
#include <limits>
#include <thrust/host_vector.h>
#include <unordered_map>

namespace faiss { namespace gpu {

IVFPQ::IVFPQ(GpuResources* resources,
             faiss::MetricType metric,
             float metricArg,
             FlatIndex* quantizer,
             int numSubQuantizers,
             int bitsPerSubQuantizer,
             float* pqCentroidData,
             IndicesOptions indicesOptions,
             bool useFloat16LookupTables,
             MemorySpace space) :
    IVFBase(resources,
            metric,
            metricArg,
            quantizer,
            numSubQuantizers,
            indicesOptions,
            space),
    numSubQuantizers_(numSubQuantizers),
    bitsPerSubQuantizer_(bitsPerSubQuantizer),
    numSubQuantizerCodes_(utils::pow2(bitsPerSubQuantizer_)),
    dimPerSubQuantizer_(dim_ / numSubQuantizers),
    precomputedCodes_(false),
    useFloat16LookupTables_(useFloat16LookupTables) {
  FAISS_ASSERT(pqCentroidData);

  FAISS_ASSERT(bitsPerSubQuantizer_ <= 8);
  FAISS_ASSERT(dim_ % numSubQuantizers_ == 0);
  FAISS_ASSERT(isSupportedPQCodeLength(bytesPerVector_));

  setPQCentroids_(pqCentroidData);
}

IVFPQ::~IVFPQ() {
}


bool
IVFPQ::isSupportedPQCodeLength(int size) {
  switch (size) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 8:
    case 12:
    case 16:
    case 20:
    case 24:
    case 28:
    case 32:
    case 40:
    case 48:
    case 56: // only supported with float16
    case 64: // only supported with float16
    case 96: // only supported with float16
      return true;
    default:
      return false;
  }
}

bool
IVFPQ::isSupportedNoPrecomputedSubDimSize(int dims) {
  return faiss::gpu::isSupportedNoPrecomputedSubDimSize(dims);
}

void
IVFPQ::setPrecomputedCodes(bool enable) {
  if (enable && metric_ == MetricType::METRIC_INNER_PRODUCT) {
    FAISS_THROW_MSG("Precomputed codes are not needed for GpuIndexIVFPQ "
                    "with METRIC_INNER_PRODUCT");
  }

  if (precomputedCodes_ != enable) {
    precomputedCodes_ = enable;

    if (precomputedCodes_) {
      precomputeCodes_();
    } else {
      // Clear out old precomputed code data
      precomputedCode_ = std::move(DeviceTensor<float, 3, true>());
      precomputedCodeHalf_ = std::move(DeviceTensor<half, 3, true>());
    }
  }
}

int
IVFPQ::classifyAndAddVectors(Tensor<float, 2, true>& vecs,
                             Tensor<long, 1, true>& indices) {
  FAISS_ASSERT(vecs.getSize(0) == indices.getSize(0));
  FAISS_ASSERT(vecs.getSize(1) == dim_);

  auto& mem = resources_->getMemoryManagerCurrentDevice();
  auto stream = resources_->getDefaultStreamCurrentDevice();

  // Number of valid vectors that we actually add; we return this
  int numAdded = 0;

  // We don't actually need this
  DeviceTensor<float, 2, true> listDistance(mem, {vecs.getSize(0), 1}, stream);
  // We use this
  DeviceTensor<int, 2, true> listIds2d(mem, {vecs.getSize(0), 1}, stream);
  auto listIds = listIds2d.view<1>({vecs.getSize(0)});

  quantizer_->query(vecs,
                    1,
                    metric_,
                    metricArg_,
                    listDistance,
                    listIds2d,
                    false);

  // Copy the lists that we wish to append to back to the CPU
  // FIXME: really this can be into pinned memory and a true async
  // copy on a different stream; we can start the copy early, but it's
  // tiny
  HostTensor<int, 1, true> listIdsHost(listIds, stream);

  // Calculate the residual for each closest centroid
  DeviceTensor<float, 2, true> residuals(
    mem, {vecs.getSize(0), vecs.getSize(1)}, stream);

  if (quantizer_->getUseFloat16()) {
    auto& coarseCentroids = quantizer_->getVectorsFloat16Ref();
    runCalcResidual(vecs, coarseCentroids, listIds, residuals, stream);
  } else {
    auto& coarseCentroids = quantizer_->getVectorsFloat32Ref();
    runCalcResidual(vecs, coarseCentroids, listIds, residuals, stream);
  }

  // Residuals are in the form
  // (vec x numSubQuantizer x dimPerSubQuantizer)
  // transpose to
  // (numSubQuantizer x vec x dimPerSubQuantizer)
  auto residualsView = residuals.view<3>(
    {residuals.getSize(0), numSubQuantizers_, dimPerSubQuantizer_});

  DeviceTensor<float, 3, true> residualsTranspose(
    mem,
    {numSubQuantizers_, residuals.getSize(0), dimPerSubQuantizer_},
    stream);

  runTransposeAny(residualsView, 0, 1, residualsTranspose, stream);

  // Get the product quantizer centroids in the form
  // (numSubQuantizer x numSubQuantizerCodes x dimPerSubQuantizer)
  // which is pqCentroidsMiddleCode_

  // We now have a batch operation to find the top-1 distances:
  // batch size: numSubQuantizer
  // centroids: (numSubQuantizerCodes x dimPerSubQuantizer)
  // residuals: (vec x dimPerSubQuantizer)
  // => (numSubQuantizer x vec x 1)

  DeviceTensor<float, 3, true> closestSubQDistance(
    mem, {numSubQuantizers_, residuals.getSize(0), 1}, stream);
  DeviceTensor<int, 3, true> closestSubQIndex(
    mem, {numSubQuantizers_, residuals.getSize(0), 1}, stream);

  for (int subQ = 0; subQ < numSubQuantizers_; ++subQ) {
    auto closestSubQDistanceView = closestSubQDistance[subQ].view();
    auto closestSubQIndexView = closestSubQIndex[subQ].view();

    auto pqCentroidsMiddleCodeView = pqCentroidsMiddleCode_[subQ].view();
    auto residualsTransposeView = residualsTranspose[subQ].view();

    runL2Distance(resources_,
                  pqCentroidsMiddleCodeView,
                  true, // pqCentroidsMiddleCodeView is row major
                  nullptr, // no precomputed norms
                  residualsTransposeView,
                  true, // residualsTransposeView is row major
                  1,
                  closestSubQDistanceView,
                  closestSubQIndexView,
                  // We don't care about distances
                  true);
  }

  // Now, we have the nearest sub-q centroid for each slice of the
  // residual vector.
  auto closestSubQIndexView = closestSubQIndex.view<2>(
    {numSubQuantizers_, residuals.getSize(0)});

  // Transpose this for easy use
  DeviceTensor<int, 2, true> encodings(
    mem, {residuals.getSize(0), numSubQuantizers_}, stream);

  runTransposeAny(closestSubQIndexView, 0, 1, encodings, stream);

  // Now we add the encoded vectors to the individual lists
  // First, make sure that there is space available for adding the new
  // encoded vectors and indices

  // list id -> # being added
  std::unordered_map<int, int> assignCounts;

  // vector id -> offset in list
  // (we already have vector id -> list id in listIds)
  HostTensor<int, 1, true> listOffsetHost({listIdsHost.getSize(0)});

  for (int i = 0; i < listIdsHost.getSize(0); ++i) {
    int listId = listIdsHost[i];

    // Add vector could be invalid (contains NaNs etc)
    if (listId < 0) {
      listOffsetHost[i] = -1;
      continue;
    }

    FAISS_ASSERT(listId < numLists_);
    ++numAdded;

    int offset = deviceListData_[listId]->size() / bytesPerVector_;

    auto it = assignCounts.find(listId);
    if (it != assignCounts.end()) {
      offset += it->second;
      it->second++;
    } else {
      assignCounts[listId] = 1;
    }

    listOffsetHost[i] = offset;
  }

  // If we didn't add anything (all invalid vectors), no need to
  // continue
  if (numAdded == 0) {
    return 0;
  }

  // We need to resize the data structures for the inverted lists on
  // the GPUs, which means that they might need reallocation, which
  // means that their base address may change. Figure out the new base
  // addresses, and update those in a batch on the device
  {
    // Resize all of the lists that we are appending to
    for (auto& counts : assignCounts) {
      auto& codes = deviceListData_[counts.first];
      codes->resize(codes->size() + counts.second * bytesPerVector_,
                    stream);
      int newNumVecs = (int) (codes->size() / bytesPerVector_);

      auto& indices = deviceListIndices_[counts.first];
      if ((indicesOptions_ == INDICES_32_BIT) ||
          (indicesOptions_ == INDICES_64_BIT)) {
        size_t indexSize =
          (indicesOptions_ == INDICES_32_BIT) ? sizeof(int) : sizeof(long);

        indices->resize(indices->size() + counts.second * indexSize, stream);
      } else if (indicesOptions_ == INDICES_CPU) {
        // indices are stored on the CPU side
        FAISS_ASSERT(counts.first < listOffsetToUserIndex_.size());

        auto& userIndices = listOffsetToUserIndex_[counts.first];
        userIndices.resize(newNumVecs);
      } else {
        // indices are not stored on the GPU or CPU side
        FAISS_ASSERT(indicesOptions_ == INDICES_IVF);
      }

      // This is used by the multi-pass query to decide how much scratch
      // space to allocate for intermediate results
      maxListLength_ = std::max(maxListLength_, newNumVecs);
    }

    // Update all pointers and sizes on the device for lists that we
    // appended to
    {
      std::vector<int> listIds(assignCounts.size());
      int i = 0;
      for (auto& counts : assignCounts) {
        listIds[i++] = counts.first;
      }

      updateDeviceListInfo_(listIds, stream);
    }
  }

  // If we're maintaining the indices on the CPU side, update our
  // map. We already resized our map above.
  if (indicesOptions_ == INDICES_CPU) {
    // We need to maintain the indices on the CPU side
    HostTensor<long, 1, true> hostIndices(indices, stream);

    for (int i = 0; i < hostIndices.getSize(0); ++i) {
      int listId = listIdsHost[i];

      // Add vector could be invalid (contains NaNs etc)
      if (listId < 0) {
        continue;
      }

      int offset = listOffsetHost[i];

      FAISS_ASSERT(listId < listOffsetToUserIndex_.size());
      auto& userIndices = listOffsetToUserIndex_[listId];

      FAISS_ASSERT(offset < userIndices.size());
      userIndices[offset] = hostIndices[i];
    }
  }

  // We similarly need to actually append the new encoded vectors
  {
    DeviceTensor<int, 1, true> listOffset(mem, listOffsetHost, stream);

    // This kernel will handle appending each encoded vector + index to
    // the appropriate list
    runIVFPQInvertedListAppend(listIds,
                               listOffset,
                               encodings,
                               indices,
                               deviceListDataPointers_,
                               deviceListIndexPointers_,
                               indicesOptions_,
                               stream);
  }

  return numAdded;
}

void
IVFPQ::addCodeVectorsFromCpu(int listId,
                             const void* codes,
                             const long* indices,
                             size_t numVecs) {
  // This list must already exist
  FAISS_ASSERT(listId < deviceListData_.size());
  auto stream = resources_->getDefaultStreamCurrentDevice();

  // If there's nothing to add, then there's nothing we have to do
  if (numVecs == 0) {
    return;
  }

  size_t lengthInBytes = numVecs * bytesPerVector_;

  auto& listCodes = deviceListData_[listId];
  auto prevCodeData = listCodes->data();

  // We only have int32 length representations on the GPU per each
  // list; the length is in sizeof(char)
  FAISS_ASSERT(listCodes->size() % bytesPerVector_ == 0);
  FAISS_ASSERT(listCodes->size() + lengthInBytes <=
               (size_t) std::numeric_limits<int>::max());

  listCodes->append((unsigned char*) codes,
                    lengthInBytes,
                    stream,
                    true /* exact reserved size */);

  // Handle the indices as well
  addIndicesFromCpu_(listId, indices, numVecs);

  // This list address may have changed due to vector resizing, but
  // only bother updating it on the device if it has changed
  if (prevCodeData != listCodes->data()) {
    deviceListDataPointers_[listId] = listCodes->data();
  }

  // And our size has changed too
  int listLength = listCodes->size() / bytesPerVector_;
  deviceListLengths_[listId] = listLength;

  // We update this as well, since the multi-pass algorithm uses it
  maxListLength_ = std::max(maxListLength_, listLength);

  // device_vector add is potentially happening on a different stream
  // than our default stream
  if (resources_->getDefaultStreamCurrentDevice() != 0) {
    streamWait({stream}, {0});
  }
}

void
IVFPQ::setPQCentroids_(float* data) {
  size_t pqSize =
    numSubQuantizers_ * numSubQuantizerCodes_ * dimPerSubQuantizer_;

  // Make sure the data is on the host
  // FIXME: why are we doing this?
  thrust::host_vector<float> hostMemory;
  hostMemory.insert(hostMemory.end(), data, data + pqSize);

  HostTensor<float, 3, true> pqHost(
    hostMemory.data(),
    {numSubQuantizers_, numSubQuantizerCodes_, dimPerSubQuantizer_});
  DeviceTensor<float, 3, true> pqDevice(
    pqHost,
    resources_->getDefaultStreamCurrentDevice());

  DeviceTensor<float, 3, true> pqDeviceTranspose(
    {numSubQuantizers_, dimPerSubQuantizer_, numSubQuantizerCodes_});
  runTransposeAny(pqDevice, 1, 2, pqDeviceTranspose,
                  resources_->getDefaultStreamCurrentDevice());

  pqCentroidsInnermostCode_ = std::move(pqDeviceTranspose);

  // Also maintain the PQ centroids in the form
  // (sub q)(code id)(sub dim)
  DeviceTensor<float, 3, true> pqCentroidsMiddleCode(
    {numSubQuantizers_, numSubQuantizerCodes_, dimPerSubQuantizer_});
  runTransposeAny(pqCentroidsInnermostCode_, 1, 2, pqCentroidsMiddleCode,
                  resources_->getDefaultStreamCurrentDevice());

  pqCentroidsMiddleCode_ = std::move(pqCentroidsMiddleCode);
}

template <typename CentroidT>
void
IVFPQ::precomputeCodesT_() {
  FAISS_ASSERT(metric_ == MetricType::METRIC_L2);

  //
  //    d = || x - y_C ||^2 + || y_R ||^2 + 2 * (y_C|y_R) - 2 * (x|y_R)
  //        ---------------   ---------------------------       -------
  //            term 1                 term 2                   term 3
  //

  // Terms 1 and 3 are available only at query time. We compute term 2
  // here.

  // Compute ||y_R||^2 by treating
  // (sub q)(code id)(sub dim) as (sub q * code id)(sub dim)
  auto pqCentroidsMiddleCodeView =
    pqCentroidsMiddleCode_.view<2>(
      {numSubQuantizers_ * numSubQuantizerCodes_, dimPerSubQuantizer_});
  DeviceTensor<float, 1, true> subQuantizerNorms(
    {numSubQuantizers_ * numSubQuantizerCodes_});

  runL2Norm(pqCentroidsMiddleCodeView, true,
            subQuantizerNorms, true,
            resources_->getDefaultStreamCurrentDevice());

  // Compute 2 * (y_C|y_R) via batch matrix multiplication
  // batch size (sub q) x {(centroid id)(sub dim) x (code id)(sub dim)'}
  //         => (sub q) x {(centroid id)(code id)}
  //         => (sub q)(centroid id)(code id)

  // View (centroid id)(dim) as
  //      (centroid id)(sub q)(dim)
  // Transpose (centroid id)(sub q)(sub dim) to
  //           (sub q)(centroid id)(sub dim)
  auto& coarseCentroids = quantizer_->template getVectorsRef<CentroidT>();
  auto centroidView = coarseCentroids.template view<3>(
    {coarseCentroids.getSize(0), numSubQuantizers_, dimPerSubQuantizer_});
  DeviceTensor<CentroidT, 3, true> centroidsTransposed(
    {numSubQuantizers_, coarseCentroids.getSize(0), dimPerSubQuantizer_});

  runTransposeAny(centroidView, 0, 1, centroidsTransposed,
                  resources_->getDefaultStreamCurrentDevice());

  DeviceTensor<float, 3, true> coarsePQProduct(
    {numSubQuantizers_, coarseCentroids.getSize(0), numSubQuantizerCodes_});

  runIteratedMatrixMult(coarsePQProduct, false,
                        centroidsTransposed, false,
                        pqCentroidsMiddleCode_, true,
                        2.0f, 0.0f,
                        resources_->getBlasHandleCurrentDevice(),
                        resources_->getDefaultStreamCurrentDevice());

  // Transpose (sub q)(centroid id)(code id) to
  //           (centroid id)(sub q)(code id)
  DeviceTensor<float, 3, true> coarsePQProductTransposed(
    {coarseCentroids.getSize(0), numSubQuantizers_, numSubQuantizerCodes_});
  runTransposeAny(coarsePQProduct, 0, 1, coarsePQProductTransposed,
                  resources_->getDefaultStreamCurrentDevice());

  // View (centroid id)(sub q)(code id) as
  //      (centroid id)(sub q * code id)
  auto coarsePQProductTransposedView = coarsePQProductTransposed.view<2>(
    {coarseCentroids.getSize(0), numSubQuantizers_ * numSubQuantizerCodes_});

  // Sum || y_R ||^2 + 2 * (y_C|y_R)
  // i.e., add norms                              (sub q * code id)
  // along columns of inner product  (centroid id)(sub q * code id)
  runSumAlongColumns(subQuantizerNorms, coarsePQProductTransposedView,
                     resources_->getDefaultStreamCurrentDevice());

  // We added into the view, so `coarsePQProductTransposed` is now our
  // precomputed term 2.
  if (useFloat16LookupTables_) {
    precomputedCodeHalf_ =
      convertTensor<float, half, 3>(resources_,
                                    resources_->getDefaultStreamCurrentDevice(),
                                    coarsePQProductTransposed);
  } else {
    precomputedCode_ = std::move(coarsePQProductTransposed);
  }
}

void
IVFPQ::precomputeCodes_() {
  if (quantizer_->getUseFloat16()) {
    precomputeCodesT_<half>();
  } else {
    precomputeCodesT_<float>();
  }
}

void
IVFPQ::query(Tensor<float, 2, true>& queries,
             int nprobe,
             int k,
             Tensor<float, 2, true>& outDistances,
             Tensor<long, 2, true>& outIndices) {
  // These are caught at a higher level
  FAISS_ASSERT(nprobe <= GPU_MAX_SELECTION_K);
  FAISS_ASSERT(k <= GPU_MAX_SELECTION_K);

  auto& mem = resources_->getMemoryManagerCurrentDevice();
  auto stream = resources_->getDefaultStreamCurrentDevice();
  nprobe = std::min(nprobe, quantizer_->getSize());

  FAISS_ASSERT(queries.getSize(1) == dim_);
  FAISS_ASSERT(outDistances.getSize(0) == queries.getSize(0));
  FAISS_ASSERT(outIndices.getSize(0) == queries.getSize(0));

  // Reserve space for the closest coarse centroids
  DeviceTensor<float, 2, true>
    coarseDistances(mem, {queries.getSize(0), nprobe}, stream);
  DeviceTensor<int, 2, true>
    coarseIndices(mem, {queries.getSize(0), nprobe}, stream);

  // Find the `nprobe` closest coarse centroids; we can use int
  // indices both internally and externally
  quantizer_->query(queries,
                    nprobe,
                    metric_,
                    metricArg_,
                    coarseDistances,
                    coarseIndices,
                    true);

  if (precomputedCodes_) {
    FAISS_ASSERT(metric_ == MetricType::METRIC_L2);

    runPQPrecomputedCodes_(queries,
                           coarseDistances,
                           coarseIndices,
                           k,
                           outDistances,
                           outIndices);
  } else {
    runPQNoPrecomputedCodes_(queries,
                             coarseDistances,
                             coarseIndices,
                             k,
                             outDistances,
                             outIndices);
  }

  // If the GPU isn't storing indices (they are on the CPU side), we
  // need to perform the re-mapping here
  // FIXME: we might ultimately be calling this function with inputs
  // from the CPU, these are unnecessary copies
  if (indicesOptions_ == INDICES_CPU) {
    HostTensor<long, 2, true> hostOutIndices(outIndices, stream);

    ivfOffsetToUserIndex(hostOutIndices.data(),
                         numLists_,
                         hostOutIndices.getSize(0),
                         hostOutIndices.getSize(1),
                         listOffsetToUserIndex_);

    // Copy back to GPU, since the input to this function is on the
    // GPU
    outIndices.copyFrom(hostOutIndices, stream);
  }
}

std::vector<unsigned char>
IVFPQ::getListCodes(int listId) const {
  FAISS_ASSERT(listId < deviceListData_.size());

  return deviceListData_[listId]->copyToHost<unsigned char>(
    resources_->getDefaultStreamCurrentDevice());
}

Tensor<float, 3, true>
IVFPQ::getPQCentroids() {
  return pqCentroidsMiddleCode_;
}

void
IVFPQ::runPQPrecomputedCodes_(
  Tensor<float, 2, true>& queries,
  DeviceTensor<float, 2, true>& coarseDistances,
  DeviceTensor<int, 2, true>& coarseIndices,
  int k,
  Tensor<float, 2, true>& outDistances,
  Tensor<long, 2, true>& outIndices) {
  FAISS_ASSERT(metric_ == MetricType::METRIC_L2);

  auto& mem = resources_->getMemoryManagerCurrentDevice();
  auto stream = resources_->getDefaultStreamCurrentDevice();

  // Compute precomputed code term 3, - 2 * (x|y_R)
  // This is done via batch MM
  // {sub q} x {(query id)(sub dim) * (code id)(sub dim)'} =>
  // {sub q} x {(query id)(code id)}
  DeviceTensor<float, 3, true> term3Transposed(
    mem,
    {queries.getSize(0), numSubQuantizers_, numSubQuantizerCodes_},
    stream);

  // These allocations within are only temporary, so release them when
  // we're done to maximize free space
  {
    auto querySubQuantizerView = queries.view<3>(
      {queries.getSize(0), numSubQuantizers_, dimPerSubQuantizer_});
    DeviceTensor<float, 3, true> queriesTransposed(
      mem,
      {numSubQuantizers_, queries.getSize(0), dimPerSubQuantizer_},
      stream);
    runTransposeAny(querySubQuantizerView, 0, 1, queriesTransposed, stream);

    DeviceTensor<float, 3, true> term3(
      mem,
      {numSubQuantizers_, queries.getSize(0), numSubQuantizerCodes_},
      stream);

    runIteratedMatrixMult(term3, false,
                          queriesTransposed, false,
                          pqCentroidsMiddleCode_, true,
                          -2.0f, 0.0f,
                          resources_->getBlasHandleCurrentDevice(),
                          stream);

    runTransposeAny(term3, 0, 1, term3Transposed, stream);
  }

  NoTypeTensor<3, true> term2;
  NoTypeTensor<3, true> term3;
  DeviceTensor<half, 3, true> term3Half;

  if (useFloat16LookupTables_) {
    term3Half =
      convertTensor<float, half, 3>(resources_, stream, term3Transposed);

    term2 = NoTypeTensor<3, true>(precomputedCodeHalf_);
    term3 = NoTypeTensor<3, true>(term3Half);
  } else {
    term2 = NoTypeTensor<3, true>(precomputedCode_);
    term3 = NoTypeTensor<3, true>(term3Transposed);
  }

  runPQScanMultiPassPrecomputed(queries,
                                coarseDistances, // term 1
                                term2, // term 2
                                term3, // term 3
                                coarseIndices,
                                useFloat16LookupTables_,
                                bytesPerVector_,
                                numSubQuantizers_,
                                numSubQuantizerCodes_,
                                deviceListDataPointers_,
                                deviceListIndexPointers_,
                                indicesOptions_,
                                deviceListLengths_,
                                maxListLength_,
                                k,
                                outDistances,
                                outIndices,
                                resources_);
}

template <typename CentroidT>
void
IVFPQ::runPQNoPrecomputedCodesT_(
  Tensor<float, 2, true>& queries,
  DeviceTensor<float, 2, true>& coarseDistances,
  DeviceTensor<int, 2, true>& coarseIndices,
  int k,
  Tensor<float, 2, true>& outDistances,
  Tensor<long, 2, true>& outIndices) {
  auto& coarseCentroids = quantizer_->template getVectorsRef<CentroidT>();

  runPQScanMultiPassNoPrecomputed(queries,
                                  coarseCentroids,
                                  pqCentroidsInnermostCode_,
                                  coarseIndices,
                                  useFloat16LookupTables_,
                                  bytesPerVector_,
                                  numSubQuantizers_,
                                  numSubQuantizerCodes_,
                                  deviceListDataPointers_,
                                  deviceListIndexPointers_,
                                  indicesOptions_,
                                  deviceListLengths_,
                                  maxListLength_,
                                  k,
                                  metric_,
                                  outDistances,
                                  outIndices,
                                  resources_);
}

void
IVFPQ::runPQNoPrecomputedCodes_(
  Tensor<float, 2, true>& queries,
  DeviceTensor<float, 2, true>& coarseDistances,
  DeviceTensor<int, 2, true>& coarseIndices,
  int k,
  Tensor<float, 2, true>& outDistances,
  Tensor<long, 2, true>& outIndices) {
  if (quantizer_->getUseFloat16()) {
    runPQNoPrecomputedCodesT_<half>(queries,
                                    coarseDistances,
                                    coarseIndices,
                                    k,
                                    outDistances,
                                    outIndices);
  } else {
    runPQNoPrecomputedCodesT_<float>(queries,
                                     coarseDistances,
                                     coarseIndices,
                                     k,
                                     outDistances,
                                     outIndices);
  }
}

} } // namespace
