#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "table/internal_iterator.h"

namespace rocksdb {

// Iterator over a vector of keys/values
class VectorIterator : public InternalIterator {
 public:
  VectorIterator(std::vector<std::string> keys, std::vector<std::string> values,
                 const InternalKeyComparator* icmp)
      : keys_(std::move(keys)),
        values_(std::move(values)),
        indexed_cmp_(icmp, &keys_),
        current_(keys.size()) {
    assert(keys_.size() == values_.size());

    indices_.reserve(keys_.size());
    for (size_t i = 0; i < keys_.size(); i++) {
      indices_.push_back(i);
    }
    std::sort(indices_.begin(), indices_.end(), indexed_cmp_);
  }

  virtual bool Valid() const override {
    return !indices_.empty() && current_ < indices_.size();
  }

  virtual void SeekToFirst() override { current_ = 0; }
  virtual void SeekToLast() override { current_ = indices_.size() - 1; }

  virtual void Seek(const Slice& target) override {
    current_ = std::lower_bound(indices_.begin(), indices_.end(), target,
                                indexed_cmp_) -
               indices_.begin();
  }

  virtual void SeekForPrev(const Slice& target) override {
    current_ = std::lower_bound(indices_.begin(), indices_.end(), target,
                                indexed_cmp_) -
               indices_.begin();
    if (!Valid()) {
      SeekToLast();
    } else {
      Prev();
    }
  }

  virtual void Next() override { current_++; }
  virtual void Prev() override { current_--; }

  virtual Slice key() const override {
    return Slice(keys_[indices_[current_]]);
  }
  virtual Slice value() const override {
    return Slice(values_[indices_[current_]]);
  }

  virtual Status status() const override { return Status::OK(); }

  virtual bool IsKeyPinned() const override { return true; }
  virtual bool IsValuePinned() const override { return true; }

 private:
  struct IndexedKeyComparator {
    IndexedKeyComparator(const InternalKeyComparator* c,
                         const std::vector<std::string>* ks)
        : cmp(c), keys(ks) {}

    bool operator()(size_t a, size_t b) const {
      return cmp->Compare((*keys)[a], (*keys)[b]) < 0;
    }

    bool operator()(size_t a, const Slice& b) const {
      return cmp->Compare((*keys)[a], b) < 0;
    }

    bool operator()(const Slice& a, size_t b) const {
      return cmp->Compare(a, (*keys)[b]) < 0;
    }

    const InternalKeyComparator* cmp;
    const std::vector<std::string>* keys;
  };

  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  IndexedKeyComparator indexed_cmp_;
  std::vector<size_t> indices_;
  size_t current_;
};

}  // namespace rocksdb
