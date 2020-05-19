/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/IVFBase.cuh>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/impl/FlatIndex.cuh>
#include <faiss/gpu/impl/IVFAppend.cuh>
#include <faiss/gpu/impl/RemapIndices.h>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/HostTensor.cuh>
#include <limits>
#include <thrust/host_vector.h>
#include <unordered_map>

namespace faiss { namespace gpu {

IVFBase::IVFBase(GpuResources* resources,
                 faiss::MetricType metric,
                 float metricArg,
                 FlatIndex* quantizer,
                 int bytesPerVector,
                 IndicesOptions indicesOptions,
                 MemorySpace space) :
    resources_(resources),
    metric_(metric),
    metricArg_(metricArg),
    quantizer_(quantizer),
    bytesPerVector_(bytesPerVector),
    indicesOptions_(indicesOptions),
    space_(space),
    dim_(quantizer->getDim()),
    numLists_(quantizer->getSize()),
    maxListLength_(0) {
  reset();
}

IVFBase::~IVFBase() {
}

void
IVFBase::reserveMemory(size_t numVecs) {
  size_t vecsPerList = numVecs / deviceListData_.size();
  if (vecsPerList < 1) {
    return;
  }

  auto stream = resources_->getDefaultStreamCurrentDevice();

  size_t bytesPerDataList = vecsPerList * bytesPerVector_;
  for (auto& list : deviceListData_) {
    list->reserve(bytesPerDataList, stream);
  }

  if ((indicesOptions_ == INDICES_32_BIT) ||
      (indicesOptions_ == INDICES_64_BIT)) {
    // Reserve for index lists as well
    size_t bytesPerIndexList = vecsPerList *
      (indicesOptions_ == INDICES_32_BIT ? sizeof(int) : sizeof(long));

    for (auto& list : deviceListIndices_) {
      list->reserve(bytesPerIndexList, stream);
    }
  }

  // Update device info for all lists, since the base pointers may
  // have changed
  updateDeviceListInfo_(stream);
}

void
IVFBase::reset() {
  deviceListData_.clear();
  deviceListIndices_.clear();
  deviceListDataPointers_.clear();
  deviceListIndexPointers_.clear();
  deviceListLengths_.clear();
  listOffsetToUserIndex_.clear();

  for (size_t i = 0; i < numLists_; ++i) {
    deviceListData_.emplace_back(
      std::unique_ptr<DeviceVector<unsigned char>>(
        new DeviceVector<unsigned char>(space_)));
    deviceListIndices_.emplace_back(
      std::unique_ptr<DeviceVector<unsigned char>>(
        new DeviceVector<unsigned char>(space_)));
    listOffsetToUserIndex_.emplace_back(std::vector<long>());
  }

  deviceListDataPointers_.resize(numLists_, nullptr);
  deviceListIndexPointers_.resize(numLists_, nullptr);
  deviceListLengths_.resize(numLists_, 0);
  maxListLength_ = 0;
}

int
IVFBase::getDim() const {
  return dim_;
}

size_t
IVFBase::reclaimMemory() {
  // Reclaim all unused memory exactly
  return reclaimMemory_(true);
}

size_t
IVFBase::reclaimMemory_(bool exact) {
  auto stream = resources_->getDefaultStreamCurrentDevice();

  size_t totalReclaimed = 0;

  for (int i = 0; i < deviceListData_.size(); ++i) {
    auto& data = deviceListData_[i];
    totalReclaimed += data->reclaim(exact, stream);

    deviceListDataPointers_[i] = data->data();
  }

  for (int i = 0; i < deviceListIndices_.size(); ++i) {
    auto& indices = deviceListIndices_[i];
    totalReclaimed += indices->reclaim(exact, stream);

    deviceListIndexPointers_[i] = indices->data();
  }

  // Update device info for all lists, since the base pointers may
  // have changed
  updateDeviceListInfo_(stream);

  return totalReclaimed;
}

void
IVFBase::updateDeviceListInfo_(cudaStream_t stream) {
  std::vector<int> listIds(deviceListData_.size());
  for (int i = 0; i < deviceListData_.size(); ++i) {
    listIds[i] = i;
  }

  updateDeviceListInfo_(listIds, stream);
}

void
IVFBase::updateDeviceListInfo_(const std::vector<int>& listIds,
                               cudaStream_t stream) {
  auto& mem = resources_->getMemoryManagerCurrentDevice();

  HostTensor<int, 1, true>
    hostListsToUpdate({(int) listIds.size()});
  HostTensor<int, 1, true>
    hostNewListLength({(int) listIds.size()});
  HostTensor<void*, 1, true>
    hostNewDataPointers({(int) listIds.size()});
  HostTensor<void*, 1, true>
    hostNewIndexPointers({(int) listIds.size()});

  for (int i = 0; i < listIds.size(); ++i) {
    auto listId = listIds[i];
    auto& data = deviceListData_[listId];
    auto& indices = deviceListIndices_[listId];

    hostListsToUpdate[i] = listId;
    hostNewListLength[i] = data->size() / bytesPerVector_;
    hostNewDataPointers[i] = data->data();
    hostNewIndexPointers[i] = indices->data();
  }

  // Copy the above update sets to the GPU
  DeviceTensor<int, 1, true> listsToUpdate(
    mem, hostListsToUpdate, stream);
  DeviceTensor<int, 1, true> newListLength(
    mem,  hostNewListLength, stream);
  DeviceTensor<void*, 1, true> newDataPointers(
    mem, hostNewDataPointers, stream);
  DeviceTensor<void*, 1, true> newIndexPointers(
    mem, hostNewIndexPointers, stream);

  // Update all pointers to the lists on the device that may have
  // changed
  runUpdateListPointers(listsToUpdate,
                        newListLength,
                        newDataPointers,
                        newIndexPointers,
                        deviceListLengths_,
                        deviceListDataPointers_,
                        deviceListIndexPointers_,
                        stream);
}

size_t
IVFBase::getNumLists() const {
  return numLists_;
}

int
IVFBase::getListLength(int listId) const {
  FAISS_ASSERT(listId < deviceListLengths_.size());

  return deviceListLengths_[listId];
}

std::vector<long>
IVFBase::getListIndices(int listId) const {
  FAISS_ASSERT(listId < numLists_);

  if (indicesOptions_ == INDICES_32_BIT) {
    FAISS_ASSERT(listId < deviceListIndices_.size());

    auto intInd = deviceListIndices_[listId]->copyToHost<int>(
      resources_->getDefaultStreamCurrentDevice());

    std::vector<long> out(intInd.size());
    for (size_t i = 0; i < intInd.size(); ++i) {
      out[i] = (long) intInd[i];
    }

    return out;
  } else if (indicesOptions_ == INDICES_64_BIT) {
    FAISS_ASSERT(listId < deviceListIndices_.size());

    return deviceListIndices_[listId]->copyToHost<long>(
      resources_->getDefaultStreamCurrentDevice());
  } else if (indicesOptions_ == INDICES_CPU) {
    FAISS_ASSERT(listId < deviceListData_.size());
    FAISS_ASSERT(listId < listOffsetToUserIndex_.size());

    auto& userIds = listOffsetToUserIndex_[listId];
    FAISS_ASSERT(userIds.size() ==
                 deviceListData_[listId]->size() / bytesPerVector_);

    // this will return a copy
    return userIds;
  } else {
    // unhandled indices type (includes INDICES_IVF)
    FAISS_ASSERT(false);
    return std::vector<long>();
  }
}

std::vector<unsigned char>
IVFBase::getListVectors(int listId) const {
  FAISS_ASSERT(listId < deviceListData_.size());
  auto& list = *deviceListData_[listId];
  auto stream = resources_->getDefaultStreamCurrentDevice();

  return list.copyToHost<unsigned char>(stream);
}

void
IVFBase::addIndicesFromCpu_(int listId,
                            const long* indices,
                            size_t numVecs) {
  auto stream = resources_->getDefaultStreamCurrentDevice();

  auto& listIndices = deviceListIndices_[listId];
  auto prevIndicesData = listIndices->data();

  if (indicesOptions_ == INDICES_32_BIT) {
    // Make sure that all indices are in bounds
    std::vector<int> indices32(numVecs);
    for (size_t i = 0; i < numVecs; ++i) {
      auto ind = indices[i];
      FAISS_ASSERT(ind <= (long) std::numeric_limits<int>::max());
      indices32[i] = (int) ind;
    }

    listIndices->append((unsigned char*) indices32.data(),
                        numVecs * sizeof(int),
                        stream,
                        true /* exact reserved size */);
  } else if (indicesOptions_ == INDICES_64_BIT) {
    listIndices->append((unsigned char*) indices,
                        numVecs * sizeof(long),
                        stream,
                        true /* exact reserved size */);
  } else if (indicesOptions_ == INDICES_CPU) {
    // indices are stored on the CPU
    FAISS_ASSERT(listId < listOffsetToUserIndex_.size());

    auto& userIndices = listOffsetToUserIndex_[listId];
    userIndices.insert(userIndices.begin(), indices, indices + numVecs);
  } else {
    // indices are not stored
    FAISS_ASSERT(indicesOptions_ == INDICES_IVF);
  }

  if (prevIndicesData != listIndices->data()) {
    deviceListIndexPointers_[listId] = listIndices->data();
  }
}

} } // namespace
