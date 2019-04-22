//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <vector>

#include "db/column_family.h"
#include "rocksdb/compaction_filter.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

const uint64_t kRangeTombstoneSentinel =
    PackSequenceAndType(kMaxSequenceNumber, kTypeRangeDeletion);

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey& b) {
  auto c = user_cmp->Compare(a.user_key(), b.user_key());
  if (c != 0) {
    return c;
  }
  auto a_footer = ExtractInternalKeyFooter(a.Encode());
  auto b_footer = ExtractInternalKeyFooter(b.Encode());
  if (a_footer == kRangeTombstoneSentinel) {
    if (b_footer != kRangeTombstoneSentinel) {
      return -1;
    }
  } else if (b_footer == kRangeTombstoneSentinel) {
    return 1;
  }
  return 0;
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey* a,
                      const InternalKey& b) {
  if (a == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, *a, b);
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey* b) {
  if (b == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, a, *b);
}

uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->fd.GetFileSize();
  }
  return sum;
}

void Compaction::SetInputVersion(Version* _input_version) {
  input_version_ = _input_version;
  cfd_ = input_version_->cfd();

  cfd_->Ref();
  input_version_->Ref();
  edit_.SetColumnFamily(cfd_->GetID());
}

void Compaction::GetBoundaryKeys(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs, Slice* smallest_user_key,
    Slice* largest_user_key) {
  bool initialized = false;
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].files.empty()) {
      continue;
    }
    if (inputs[i].level == 0) {
      // we need to consider all files on level 0
      for (const auto* f : inputs[i].files) {
        const Slice& start_user_key = f->smallest.user_key();
        if (!initialized ||
            ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
          *smallest_user_key = start_user_key;
        }
        const Slice& end_user_key = f->largest.user_key();
        if (!initialized ||
            ucmp->Compare(end_user_key, *largest_user_key) > 0) {
          *largest_user_key = end_user_key;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      const Slice& start_user_key = inputs[i].files[0]->smallest.user_key();
      if (!initialized ||
          ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
        *smallest_user_key = start_user_key;
      }
      const Slice& end_user_key = inputs[i].files.back()->largest.user_key();
      if (!initialized || ucmp->Compare(end_user_key, *largest_user_key) > 0) {
        *largest_user_key = end_user_key;
      }
      initialized = true;
    }
  }
}

std::vector<CompactionInputFiles> Compaction::PopulateWithAtomicBoundaries(
    VersionStorageInfo* vstorage, std::vector<CompactionInputFiles> inputs) {
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); i++) {
    if (inputs[i].level == 0 || inputs[i].files.empty()) {
      continue;
    }
    inputs[i].atomic_compaction_unit_boundaries.reserve(inputs[i].files.size());
    AtomicCompactionUnitBoundary cur_boundary;
    size_t first_atomic_idx = 0;
    auto add_unit_boundary = [&](size_t to) {
      if (first_atomic_idx == to) return;
      for (size_t k = first_atomic_idx; k < to; k++) {
        inputs[i].atomic_compaction_unit_boundaries.push_back(cur_boundary);
      }
      first_atomic_idx = to;
    };
    for (size_t j = 0; j < inputs[i].files.size(); j++) {
      const auto* f = inputs[i].files[j];
      if (j == 0) {
        // First file in a level.
        cur_boundary.smallest = &f->smallest;
        cur_boundary.largest = &f->largest;
      } else if (sstableKeyCompare(ucmp, *cur_boundary.largest, f->smallest) ==
                 0) {
        // SSTs overlap but the end key of the previous file was not
        // artificially extended by a range tombstone. Extend the current
        // boundary.
        cur_boundary.largest = &f->largest;
      } else {
        // Atomic compaction unit has ended.
        add_unit_boundary(j);
        cur_boundary.smallest = &f->smallest;
        cur_boundary.largest = &f->largest;
      }
    }
    add_unit_boundary(inputs[i].files.size());
    assert(inputs[i].files.size() ==
           inputs[i].atomic_compaction_unit_boundaries.size());
  }
  return inputs;
}

// helper function to determine if compaction is creating files at the
// bottommost level
bool Compaction::IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  int output_l0_idx;
  if (output_level == 0) {
    output_l0_idx = 0;
    for (const auto* file : vstorage->LevelFiles(0)) {
      if (inputs[0].files.back() == file) {
        break;
      }
      ++output_l0_idx;
    }
    assert(static_cast<size_t>(output_l0_idx) < vstorage->LevelFiles(0).size());
  } else {
    output_l0_idx = -1;
  }
  Slice smallest_key, largest_key;
  GetBoundaryKeys(vstorage, inputs, &smallest_key, &largest_key);
  return !vstorage->RangeMightExistAfterSortedRun(smallest_key, largest_key,
                                                  output_level, output_l0_idx);
}

// test function to validate the functionality of IsBottommostLevel()
// function -- determines if compaction with inputs and storage is bottommost
bool Compaction::TEST_IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  return IsBottommostLevel(output_level, vstorage, inputs);
}

bool Compaction::IsFullCompaction(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  size_t num_files_in_compaction = 0;
  size_t total_num_files = 0;
  for (int l = 0; l < vstorage->num_levels(); l++) {
    total_num_files += vstorage->NumLevelFiles(l);
  }
  for (size_t i = 0; i < inputs.size(); i++) {
    num_files_in_compaction += inputs[i].size();
  }
  return num_files_in_compaction == total_num_files;
}

Compaction::Compaction(VersionStorageInfo* vstorage,
                       const ImmutableCFOptions& _immutable_cf_options,
                       const MutableCFOptions& _mutable_cf_options,
                       std::vector<CompactionInputFiles> _inputs,
                       int _output_level, uint64_t _target_file_size,
                       uint64_t _max_compaction_bytes, uint32_t _output_path_id,
                       CompressionType _compression,
                       CompressionOptions _compression_opts,
                       uint32_t _max_subcompactions,
                       std::vector<FileMetaData*> _grandparents,
                       bool _manual_compaction, double _score,
                       bool _deletion_compaction,
                       CompactionReason _compaction_reason)
    : input_vstorage_(vstorage),
      start_level_(_inputs[0].level),
      output_level_(_output_level),
      max_output_file_size_(_target_file_size),
      max_compaction_bytes_(_max_compaction_bytes),
      max_subcompactions_(_max_subcompactions),
      immutable_cf_options_(_immutable_cf_options),
      mutable_cf_options_(_mutable_cf_options),
      input_version_(nullptr),
      number_levels_(vstorage->num_levels()),
      cfd_(nullptr),
      output_path_id_(_output_path_id),
      output_compression_(_compression),
      output_compression_opts_(_compression_opts),
      deletion_compaction_(_deletion_compaction),
      inputs_(PopulateWithAtomicBoundaries(vstorage, std::move(_inputs))),
      grandparents_(std::move(_grandparents)),
      score_(_score),
      bottommost_level_(IsBottommostLevel(output_level_, vstorage, inputs_)),
      is_full_compaction_(IsFullCompaction(vstorage, inputs_)),
      is_manual_compaction_(_manual_compaction),
      is_trivial_move_(false),
      compaction_reason_(_compaction_reason) {
  MarkFilesBeingCompacted(true);
  if (is_manual_compaction_) {
    compaction_reason_ = CompactionReason::kManualCompaction;
  }
  if (max_subcompactions_ == 0) {
    max_subcompactions_ = immutable_cf_options_.max_subcompactions;
  }
  if (!bottommost_level_) {
    // Currently we only enable dictionary compression during compaction to the
    // bottommost level.
    output_compression_opts_.max_dict_bytes = 0;
    output_compression_opts_.zstd_max_train_bytes = 0;
  }

#ifndef NDEBUG
  for (size_t i = 1; i < inputs_.size(); ++i) {
    assert(inputs_[i].level > inputs_[i - 1].level);
  }
#endif

  // setup input_levels_
  {
    input_levels_.resize(num_input_levels());
    for (size_t which = 0; which < num_input_levels(); which++) {
      DoGenerateLevelFilesBrief(&input_levels_[which], inputs_[which].files,
                                &arena_);
    }
  }

  GetBoundaryKeys(vstorage, inputs_, &smallest_user_key_, &largest_user_key_);
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  if (cfd_ != nullptr) {
    if (cfd_->Unref()) {
      delete cfd_;
    }
  }
}

bool Compaction::InputCompressionMatchesOutput() const {
  int base_level = input_vstorage_->base_level();
  bool matches = (GetCompressionType(immutable_cf_options_, input_vstorage_,
                                     mutable_cf_options_, start_level_,
                                     base_level) == output_compression_);
  if (matches) {
    TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:Matches");
    return true;
  }
  TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:DidntMatch");
  return matches;
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // If start_level_== output_level_, the purpose is to force compaction
  // filter to be applied to that level, and thus cannot be a trivial move.

  // Check if start level have files with overlapping ranges
  if (start_level_ == 0 && input_vstorage_->level0_non_overlapping() == false) {
    // We cannot move files from L0 to L1 if the files are overlapping
    return false;
  }

  if (is_manual_compaction_ &&
      (immutable_cf_options_.compaction_filter != nullptr ||
       immutable_cf_options_.compaction_filter_factory != nullptr)) {
    // This is a manual compaction and we have a compaction filter that should
    // be executed, we cannot do a trivial move
    return false;
  }

  // Used in universal compaction, where trivial move can be done if the
  // input files are non overlapping
  if ((mutable_cf_options_.compaction_options_universal.allow_trivial_move) &&
      (output_level_ != 0)) {
    return is_trivial_move_;
  }

  if (!(start_level_ != output_level_ && num_input_levels() == 1 &&
          input(0, 0)->fd.GetPathId() == output_path_id() &&
          InputCompressionMatchesOutput())) {
    return false;
  }

  // assert inputs_.size() == 1

  for (const auto& file : inputs_.front().files) {
    std::vector<FileMetaData*> file_grand_parents;
    if (output_level_ + 1 >= number_levels_) {
      continue;
    }
    input_vstorage_->GetOverlappingInputs(output_level_ + 1, &file->smallest,
                                          &file->largest, &file_grand_parents);
    const auto compaction_size =
        file->fd.GetFileSize() + TotalFileSize(file_grand_parents);
    if (compaction_size > max_compaction_bytes_) {
      return false;
    }
  }

  return true;
}

void Compaction::AddInputDeletions(VersionEdit* out_edit) {
  for (size_t which = 0; which < num_input_levels(); which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      out_edit->DeleteFile(level(which), inputs_[which][i]->fd.GetNumber());
    }
  }
}

bool Compaction::KeyNotExistsBeyondOutputLevel(
    const Slice& user_key, std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  if (bottommost_level_) {
    return true;
  } else if (output_level_ != 0 &&
             cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_->LevelFiles(lvl);
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // We've advanced far enough
          if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
            // Key falls in this file's range, so it may
            // exist beyond output level
            return false;
          }
          break;
        }
      }
    }
    return true;
  }
  return false;
}

// Mark (or clear) each file that is being compacted
void Compaction::MarkFilesBeingCompacted(bool mark_as_compacted) {
  for (size_t i = 0; i < num_input_levels(); i++) {
    for (size_t j = 0; j < inputs_[i].size(); j++) {
      assert(mark_as_compacted ? !inputs_[i][j]->being_compacted
                               : inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = mark_as_compacted;
    }
  }
}

// Sample output:
// If compacting 3 L0 files, 2 L3 files and 1 L4 file, and outputting to L5,
// print: "3@0 + 2@3 + 1@4 files to L5"
const char* Compaction::InputLevelSummary(
    InputLevelSummaryBuffer* scratch) const {
  int len = 0;
  bool is_first = true;
  for (auto& input_level : inputs_) {
    if (input_level.empty()) {
      continue;
    }
    if (!is_first) {
      len +=
          snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, " + ");
      len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
    } else {
      is_first = false;
    }
    len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                    "%" ROCKSDB_PRIszt "@%d", input_level.size(),
                    input_level.level);
    len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
           " files to L%d", output_level());

  return scratch->buffer;
}

uint64_t Compaction::CalculateTotalInputSize() const {
  uint64_t size = 0;
  for (auto& input_level : inputs_) {
    for (auto f : input_level.files) {
      size += f->fd.GetFileSize();
    }
  }
  return size;
}

void Compaction::ReleaseCompactionFiles(Status status) {
  MarkFilesBeingCompacted(false);
  cfd_->compaction_picker()->ReleaseCompactionFiles(this, status);
}

void Compaction::ResetNextCompactionIndex() {
  assert(input_version_ != nullptr);
  input_vstorage_->ResetNextCompactionIndex(start_level_);
}

namespace {
int InputSummary(const std::vector<FileMetaData*>& files, char* output,
                 int len) {
  *output = '\0';
  int write = 0;
  for (size_t i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret;
    char sztxt[16];
    AppendHumanBytes(files.at(i)->fd.GetFileSize(), sztxt, 16);
    ret = snprintf(output + write, sz, "%" PRIu64 "(%s) ",
                   files.at(i)->fd.GetNumber(), sztxt);
    if (ret < 0 || ret >= sz) break;
    write += ret;
  }
  // if files.size() is non-zero, overwrite the last space
  return write - !!files.size();
}
}  // namespace

void Compaction::Summary(char* output, int len) {
  int write =
      snprintf(output, len, "Base version %" PRIu64 " Base level %d, inputs: [",
               input_version_->GetVersionNumber(), start_level_);
  if (write < 0 || write >= len) {
    return;
  }

  for (size_t level_iter = 0; level_iter < num_input_levels(); ++level_iter) {
    if (level_iter > 0) {
      write += snprintf(output + write, len - write, "], [");
      if (write < 0 || write >= len) {
        return;
      }
    }
    write +=
        InputSummary(inputs_[level_iter].files, output + write, len - write);
    if (write < 0 || write >= len) {
      return;
    }
  }

  snprintf(output + write, len - write, "]");
}

uint64_t Compaction::OutputFilePreallocationSize() const {
  uint64_t preallocation_size = 0;

  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      preallocation_size += file->fd.GetFileSize();
    }
  }

  if (max_output_file_size_ != port::kMaxUint64 &&
      (immutable_cf_options_.compaction_style == kCompactionStyleLevel ||
       output_level() > 0)) {
    preallocation_size = std::min(max_output_file_size_, preallocation_size);
  }

  // Over-estimate slightly so we don't end up just barely crossing
  // the threshold
  // No point to prellocate more than 1GB.
  return std::min(uint64_t{1073741824},
                  preallocation_size + (preallocation_size / 10));
}

std::unique_ptr<CompactionFilter> Compaction::CreateCompactionFilter() const {
  if (!cfd_->ioptions()->compaction_filter_factory) {
    return nullptr;
  }

  CompactionFilter::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.column_family_id = cfd_->GetID();
  return cfd_->ioptions()->compaction_filter_factory->CreateCompactionFilter(
      context);
}

bool Compaction::IsOutputLevelEmpty() const {
  return inputs_.back().level != output_level_ || inputs_.back().empty();
}

bool Compaction::ShouldFormSubcompactions() const {
  if (max_subcompactions_ <= 1 || cfd_ == nullptr) {
    return false;
  }
  if (cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    return (start_level_ == 0 || is_manual_compaction_) && output_level_ > 0 &&
           !IsOutputLevelEmpty();
  } else if (cfd_->ioptions()->compaction_style == kCompactionStyleUniversal) {
    return number_levels_ > 1 && output_level_ > 0;
  } else {
    return false;
  }
}

uint64_t Compaction::MaxInputFileCreationTime() const {
  uint64_t max_creation_time = 0;
  for (const auto& file : inputs_[0].files) {
    if (file->fd.table_reader != nullptr &&
        file->fd.table_reader->GetTableProperties() != nullptr) {
      uint64_t creation_time =
          file->fd.table_reader->GetTableProperties()->creation_time;
      max_creation_time = std::max(max_creation_time, creation_time);
    }
  }
  return max_creation_time;
}

int Compaction::GetInputBaseLevel() const {
  return input_vstorage_->base_level();
}

}  // namespace rocksdb
