/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/utils/StackDeviceMemory.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/MemorySpace.h>
#include <faiss/gpu/utils/StaticUtils.h>
#include <faiss/impl/FaissAssert.h>
#include <stdio.h>
#include <sstream>

namespace faiss { namespace gpu {

StackDeviceMemory::Stack::Stack(int d, size_t sz)
    : device_(d),
      isOwner_(true),
      start_(nullptr),
      end_(nullptr),
      size_(sz),
      head_(nullptr),
      mallocCurrent_(0),
      highWaterMemoryUsed_(0),
      highWaterMalloc_(0),
      cudaMallocWarning_(true) {
  DeviceScope s(device_);

  allocMemorySpace(MemorySpace::Device, &start_, size_);

  head_ = start_;
  end_ = start_ + size_;
}

StackDeviceMemory::Stack::Stack(int d, void* p, size_t sz, bool isOwner)
    : device_(d),
      isOwner_(isOwner),
      start_((char*) p),
      end_(((char*) p) + sz),
      size_(sz),
      head_((char*) p),
      mallocCurrent_(0),
      highWaterMemoryUsed_(0),
      highWaterMalloc_(0),
      cudaMallocWarning_(true) {
}

StackDeviceMemory::Stack::~Stack() {
  if (isOwner_) {
    DeviceScope s(device_);

    freeMemorySpace(MemorySpace::Device, start_);
  }
}

size_t
StackDeviceMemory::Stack::getSizeAvailable() const {
  return (end_ - head_);
}

char*
StackDeviceMemory::Stack::getAlloc(size_t size,
                                   cudaStream_t stream) {
  if (size > (end_ - head_)) {
    // Too large for our stack
    DeviceScope s(device_);

    if (cudaMallocWarning_) {
      // Print our requested size before we attempt the allocation
      fprintf(stderr, "WARN: increase temp memory to avoid cudaMalloc, "
              "or decrease query/add size (alloc %zu B, highwater %zu B)\n",
              size, highWaterMalloc_);
    }

    char* p = nullptr;
    allocMemorySpace(MemorySpace::Device, &p, size);

    mallocCurrent_ += size;
    highWaterMalloc_ = std::max(highWaterMalloc_, mallocCurrent_);

    return p;
  } else {
    // We can make the allocation out of our stack
    // Find all the ranges that we overlap that may have been
    // previously allocated; our allocation will be [head, endAlloc)
    char* startAlloc = head_;
    char* endAlloc = head_ + size;

    while (lastUsers_.size() > 0) {
      auto& prevUser = lastUsers_.back();

      // Because there is a previous user, we must overlap it
      FAISS_ASSERT(prevUser.start_ <= endAlloc && prevUser.end_ >= startAlloc);

      if (stream != prevUser.stream_) {
        // Synchronization required
        // FIXME
        FAISS_ASSERT(false);
      }

      if (endAlloc < prevUser.end_) {
        // Update the previous user info
        prevUser.start_ = endAlloc;

        break;
      }

      // If we're the exact size of the previous request, then we
      // don't need to continue
      bool done = (prevUser.end_ == endAlloc);

      lastUsers_.pop_back();

      if (done) {
        break;
      }
    }

    head_ = endAlloc;
    FAISS_ASSERT(head_ <= end_);

    highWaterMemoryUsed_ = std::max(highWaterMemoryUsed_,
                                    (size_t) (head_ - start_));
    return startAlloc;
  }
}

void
StackDeviceMemory::Stack::returnAlloc(char* p,
                                      size_t size,
                                      cudaStream_t stream) {
  if (p < start_ || p >= end_) {
    // This is not on our stack; it was a one-off allocation
    DeviceScope s(device_);

    freeMemorySpace(MemorySpace::Device, p);

    FAISS_ASSERT(mallocCurrent_ >= size);
    mallocCurrent_ -= size;
  } else {
    // This is on our stack
    // Allocations should be freed in the reverse order they are made
    FAISS_ASSERT(p + size == head_);

    head_ = p;
    lastUsers_.push_back(Range(p, p + size, stream));
  }
}

std::string
StackDeviceMemory::Stack::toString() const {
  std::stringstream s;

  s << "SDM device " << device_ << ": Total memory " << size_ << " ["
    << (void*) start_ << ", " << (void*) end_ << ")\n";
  s << "     Available memory " << (size_t) (end_ - head_)
    << " [" << (void*) head_ << ", " << (void*) end_ << ")\n";
  s << "     High water temp alloc " << highWaterMemoryUsed_ << "\n";
  s << "     High water cudaMalloc " << highWaterMalloc_ << "\n";

  int i = lastUsers_.size();
  for (auto it = lastUsers_.rbegin(); it != lastUsers_.rend(); ++it) {
    s << i-- << ": size " << (size_t) (it->end_ - it->start_)
      << " stream " << it->stream_
      << " [" << (void*) it->start_ << ", " << (void*) it->end_ << ")\n";
  }

  return s.str();
}

size_t
StackDeviceMemory::Stack::getHighWaterCudaMalloc() const {
  return highWaterMalloc_;
}

StackDeviceMemory::StackDeviceMemory(int device, size_t allocPerDevice)
    : device_(device),
      stack_(device, allocPerDevice) {
}

StackDeviceMemory::StackDeviceMemory(int device,
                                     void* p, size_t size, bool isOwner)
    : device_(device),
      stack_(device, p, size, isOwner) {
}

StackDeviceMemory::~StackDeviceMemory() {
}

void
StackDeviceMemory::setCudaMallocWarning(bool b) {
  stack_.cudaMallocWarning_ = b;
}

int
StackDeviceMemory::getDevice() const {
  return device_;
}

DeviceMemoryReservation
StackDeviceMemory::getMemory(cudaStream_t stream, size_t size) {
  // We guarantee 16 byte alignment for allocations, so bump up `size`
  // to the next highest multiple of 16
  size = utils::roundUp(size, (size_t) 16);

  return DeviceMemoryReservation(this,
                                 device_,
                                 stack_.getAlloc(size, stream),
                                 size,
                                 stream);
}

size_t
StackDeviceMemory::getSizeAvailable() const {
  return stack_.getSizeAvailable();
}

std::string
StackDeviceMemory::toString() const {
  return stack_.toString();
}

size_t
StackDeviceMemory::getHighWaterCudaMalloc() const {
  return stack_.getHighWaterCudaMalloc();
}

void
StackDeviceMemory::returnAllocation(DeviceMemoryReservation& m) {
  FAISS_ASSERT(m.get());
  FAISS_ASSERT(device_ == m.device());

  stack_.returnAlloc((char*) m.get(), m.size(), m.stream());
}

} } // namespace
