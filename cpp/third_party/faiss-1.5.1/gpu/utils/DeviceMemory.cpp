/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "DeviceMemory.h"
#include "DeviceUtils.h"
#include "../../FaissAssert.h"

namespace faiss { namespace gpu {

DeviceMemoryReservation::DeviceMemoryReservation()
    : state_(NULL),
      device_(0),
      data_(NULL),
      size_(0),
      stream_(0) {
}

DeviceMemoryReservation::DeviceMemoryReservation(DeviceMemory* state,
                                             int device,
                                             void* p,
                                             size_t size,
                                             cudaStream_t stream)
    : state_(state),
      device_(device),
      data_(p),
      size_(size),
      stream_(stream) {
}

DeviceMemoryReservation::DeviceMemoryReservation(
  DeviceMemoryReservation&& m) noexcept {

  state_ = m.state_;
  device_ = m.device_;
  data_ = m.data_;
  size_ = m.size_;
  stream_ = m.stream_;

  m.data_ = NULL;
}

DeviceMemoryReservation::~DeviceMemoryReservation() {
  if (data_) {
    FAISS_ASSERT(state_);
    state_->returnAllocation(*this);
  }

  data_ = NULL;
}

DeviceMemoryReservation&
DeviceMemoryReservation::operator=(DeviceMemoryReservation&& m) {
  if (data_) {
    FAISS_ASSERT(state_);
    state_->returnAllocation(*this);
  }

  state_ = m.state_;
  device_ = m.device_;
  data_ = m.data_;
  size_ = m.size_;
  stream_ = m.stream_;

  m.data_ = NULL;

  return *this;
}

DeviceMemory::~DeviceMemory() {
}

} } // namespace
