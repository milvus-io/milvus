// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_BUFFER_H
#define ARROW_BUFFER_H

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

// ----------------------------------------------------------------------
// Buffer classes

/// \class Buffer
/// \brief Object containing a pointer to a piece of contiguous memory with a
/// particular size.
///
/// Buffers have two related notions of length: size and capacity. Size is
/// the number of bytes that might have valid data. Capacity is the number
/// of bytes that were allocated for the buffer in total.
///
/// The Buffer base class does not own its memory, but subclasses often do.
///
/// The following invariant is always true: Size <= Capacity
class ARROW_EXPORT Buffer {
 public:
  /// \brief Construct from buffer and size without copying memory
  ///
  /// \param[in] data a memory buffer
  /// \param[in] size buffer size
  ///
  /// \note The passed memory must be kept alive through some other means
  Buffer(const uint8_t* data, int64_t size)
      : is_mutable_(false),
        data_(data),
        mutable_data_(NULLPTR),
        size_(size),
        capacity_(size) {}

  /// \brief Construct from string_view without copying memory
  ///
  /// \param[in] data a string_view object
  ///
  /// \note The memory viewed by data must not be deallocated in the lifetime of the
  /// Buffer; temporary rvalue strings must be stored in an lvalue somewhere
  explicit Buffer(util::string_view data)
      : Buffer(reinterpret_cast<const uint8_t*>(data.data()),
               static_cast<int64_t>(data.size())) {}

  virtual ~Buffer() = default;

  /// An offset into data that is owned by another buffer, but we want to be
  /// able to retain a valid pointer to it even after other shared_ptr's to the
  /// parent buffer have been destroyed
  ///
  /// This method makes no assertions about alignment or padding of the buffer but
  /// in general we expected buffers to be aligned and padded to 64 bytes.  In the future
  /// we might add utility methods to help determine if a buffer satisfies this contract.
  Buffer(const std::shared_ptr<Buffer>& parent, const int64_t offset, const int64_t size)
      : Buffer(parent->data() + offset, size) {
    parent_ = parent;
  }

  uint8_t operator[](std::size_t i) const { return data_[i]; }

  bool is_mutable() const { return is_mutable_; }

  /// \brief Construct a new std::string with a hexadecimal representation of the buffer.
  /// \return std::string
  std::string ToHexString();

  /// Return true if both buffers are the same size and contain the same bytes
  /// up to the number of compared bytes
  bool Equals(const Buffer& other, int64_t nbytes) const;

  /// Return true if both buffers are the same size and contain the same bytes
  bool Equals(const Buffer& other) const;

  /// Copy a section of the buffer into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes, MemoryPool* pool,
              std::shared_ptr<Buffer>* out) const;

  /// Copy a section of the buffer using the default memory pool into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes,
              std::shared_ptr<Buffer>* out) const;

  /// Zero bytes in padding, i.e. bytes between size_ and capacity_.
  void ZeroPadding() {
#ifndef NDEBUG
    CheckMutable();
#endif
    // A zero-capacity buffer can have a null data pointer
    if (capacity_ != 0) {
      memset(mutable_data_ + size_, 0, static_cast<size_t>(capacity_ - size_));
    }
  }

  /// \brief Construct a new buffer that owns its memory from a std::string
  ///
  /// \param[in] data a std::string object
  /// \param[in] pool a memory pool
  /// \param[out] out the created buffer
  ///
  /// \return Status message
  static Status FromString(const std::string& data, MemoryPool* pool,
                           std::shared_ptr<Buffer>* out);

  /// \brief Construct a new buffer that owns its memory from a std::string
  /// using the default memory pool
  static Status FromString(const std::string& data, std::shared_ptr<Buffer>* out);

  /// \brief Construct an immutable buffer that takes ownership of the contents
  /// of an std::string
  /// \param[in] data an rvalue-reference of a string
  /// \return a new Buffer instance
  static std::shared_ptr<Buffer> FromString(std::string&& data);

  /// \brief Create buffer referencing typed memory with some length without
  /// copying
  /// \param[in] data the typed memory as C array
  /// \param[in] length the number of values in the array
  /// \return a new shared_ptr<Buffer>
  template <typename T, typename SizeType = int64_t>
  static std::shared_ptr<Buffer> Wrap(const T* data, SizeType length) {
    return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data),
                                    static_cast<int64_t>(sizeof(T) * length));
  }

  /// \brief Create buffer referencing std::vector with some length without
  /// copying
  /// \param[in] data the vector to be referenced. If this vector is changed,
  /// the buffer may become invalid
  /// \return a new shared_ptr<Buffer>
  template <typename T>
  static std::shared_ptr<Buffer> Wrap(const std::vector<T>& data) {
    return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data.data()),
                                    static_cast<int64_t>(sizeof(T) * data.size()));
  }

  /// \brief Copy buffer contents into a new std::string
  /// \return std::string
  /// \note Can throw std::bad_alloc if buffer is large
  std::string ToString() const;

  /// \brief View buffer contents as a util::string_view
  /// \return util::string_view
  explicit operator util::string_view() const {
    return util::string_view(reinterpret_cast<const char*>(data_), size_);
  }

  /// \brief Return a pointer to the buffer's data
  const uint8_t* data() const { return data_; }
  /// \brief Return a writable pointer to the buffer's data
  ///
  /// The buffer has to be mutable.  Otherwise, an assertion may be thrown
  /// or a null pointer may be returned.
  uint8_t* mutable_data() {
#ifndef NDEBUG
    CheckMutable();
#endif
    return mutable_data_;
  }

  /// \brief Return the buffer's size in bytes
  int64_t size() const { return size_; }

  /// \brief Return the buffer's capacity (number of allocated bytes)
  int64_t capacity() const { return capacity_; }

  std::shared_ptr<Buffer> parent() const { return parent_; }

 protected:
  bool is_mutable_;
  const uint8_t* data_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t capacity_;

  // null by default, but may be set
  std::shared_ptr<Buffer> parent_;

  void CheckMutable() const;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Buffer);
};

using BufferVector = std::vector<std::shared_ptr<Buffer>>;

/// \defgroup buffer-slicing-functions Functions for slicing buffers
///
/// @{

/// \brief Construct a view on a buffer at the given offset and length.
///
/// This function cannot fail and does not check for errors (except in debug builds)
static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset,
                                                  const int64_t length) {
  return std::make_shared<Buffer>(buffer, offset, length);
}

/// \brief Construct a view on a buffer at the given offset, up to the buffer's end.
///
/// This function cannot fail and does not check for errors (except in debug builds)
static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset) {
  int64_t length = buffer->size() - offset;
  return SliceBuffer(buffer, offset, length);
}

/// \brief Like SliceBuffer, but construct a mutable buffer slice.
///
/// If the parent buffer is not mutable, behavior is undefined (it may abort
/// in debug builds).
ARROW_EXPORT
std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length);

/// \brief Like SliceBuffer, but construct a mutable buffer slice.
///
/// If the parent buffer is not mutable, behavior is undefined (it may abort
/// in debug builds).
static inline std::shared_ptr<Buffer> SliceMutableBuffer(
    const std::shared_ptr<Buffer>& buffer, const int64_t offset) {
  int64_t length = buffer->size() - offset;
  return SliceMutableBuffer(buffer, offset, length);
}

/// @}

/// \class MutableBuffer
/// \brief A Buffer whose contents can be mutated. May or may not own its data.
class ARROW_EXPORT MutableBuffer : public Buffer {
 public:
  MutableBuffer(uint8_t* data, const int64_t size) : Buffer(data, size) {
    mutable_data_ = data;
    is_mutable_ = true;
  }

  MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                const int64_t size);

  /// \brief Create buffer referencing typed memory with some length
  /// \param[in] data the typed memory as C array
  /// \param[in] length the number of values in the array
  /// \return a new shared_ptr<Buffer>
  template <typename T, typename SizeType = int64_t>
  static std::shared_ptr<Buffer> Wrap(T* data, SizeType length) {
    return std::make_shared<MutableBuffer>(reinterpret_cast<uint8_t*>(data),
                                           static_cast<int64_t>(sizeof(T) * length));
  }

 protected:
  MutableBuffer() : Buffer(NULLPTR, 0) {}
};

/// \class ResizableBuffer
/// \brief A mutable buffer that can be resized
class ARROW_EXPORT ResizableBuffer : public MutableBuffer {
 public:
  /// Change buffer reported size to indicated size, allocating memory if
  /// necessary.  This will ensure that the capacity of the buffer is a multiple
  /// of 64 bytes as defined in Layout.md.
  /// Consider using ZeroPadding afterwards, to conform to the Arrow layout
  /// specification.
  ///
  /// @param new_size The new size for the buffer.
  /// @param shrink_to_fit Whether to shrink the capacity if new size < current size
  virtual Status Resize(const int64_t new_size, bool shrink_to_fit = true) = 0;

  /// Ensure that buffer has enough memory allocated to fit the indicated
  /// capacity (and meets the 64 byte padding requirement in Layout.md).
  /// It does not change buffer's reported size and doesn't zero the padding.
  virtual Status Reserve(const int64_t new_capacity) = 0;

  template <class T>
  Status TypedResize(const int64_t new_nb_elements, bool shrink_to_fit = true) {
    return Resize(sizeof(T) * new_nb_elements, shrink_to_fit);
  }

  template <class T>
  Status TypedReserve(const int64_t new_nb_elements) {
    return Reserve(sizeof(T) * new_nb_elements);
  }

 protected:
  ResizableBuffer(uint8_t* data, int64_t size) : MutableBuffer(data, size) {}
};

/// \defgroup buffer-allocation-functions Functions for allocating buffers
///
/// @{

/// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::shared_ptr<Buffer>* out);

/// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::unique_ptr<Buffer>* out);

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(const int64_t size, std::shared_ptr<Buffer>* out);

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(const int64_t size, std::unique_ptr<Buffer>* out);

/// \brief Allocate a resizeable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::shared_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::unique_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(const int64_t size, std::shared_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(const int64_t size, std::unique_ptr<ResizableBuffer>* out);

/// \brief Allocate a bitmap buffer from a memory pool
/// no guarantee on values is provided.
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateBitmap(MemoryPool* pool, int64_t length, std::shared_ptr<Buffer>* out);

/// \brief Allocate a zero-initialized bitmap buffer from a memory pool
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer (zero-initialized).
///
/// \return Status message
ARROW_EXPORT
Status AllocateEmptyBitmap(MemoryPool* pool, int64_t length,
                           std::shared_ptr<Buffer>* out);

/// \brief Allocate a zero-initialized bitmap buffer from the default memory pool
///
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateEmptyBitmap(int64_t length, std::shared_ptr<Buffer>* out);

/// \brief Concatenate multiple buffers into a single buffer
///
/// \param[in] buffers to be concatenated
/// \param[in] pool memory pool to allocate the new buffer from
/// \param[out] out the concatenated buffer
///
/// \return Status
ARROW_EXPORT
Status ConcatenateBuffers(const BufferVector& buffers, MemoryPool* pool,
                          std::shared_ptr<Buffer>* out);

/// @}

}  // namespace arrow

#endif  // ARROW_BUFFER_H
