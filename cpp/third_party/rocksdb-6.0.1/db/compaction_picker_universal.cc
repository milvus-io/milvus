//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_picker_universal.h"
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include "db/column_family.h"
#include "monitoring/statistics.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace {
// Used in universal compaction when trivial move is enabled.
// This structure is used for the construction of min heap
// that contains the file meta data, the level of the file
// and the index of the file in that level

struct InputFileInfo {
  InputFileInfo() : f(nullptr), level(0), index(0) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};

// Used in universal compaction when trivial move is enabled.
// This comparator is used for the construction of min heap
// based on the smallest key of the file.
struct SmallestKeyHeapComparator {
  explicit SmallestKeyHeapComparator(const Comparator* ucmp) { ucmp_ = ucmp; }

  bool operator()(InputFileInfo i1, InputFileInfo i2) const {
    return (ucmp_->Compare(i1.f->smallest.user_key(),
                           i2.f->smallest.user_key()) > 0);
  }

 private:
  const Comparator* ucmp_;
};

typedef std::priority_queue<InputFileInfo, std::vector<InputFileInfo>,
                            SmallestKeyHeapComparator>
    SmallestKeyHeap;

// This function creates the heap that is used to find if the files are
// overlapping during universal compaction when the allow_trivial_move
// is set.
SmallestKeyHeap create_level_heap(Compaction* c, const Comparator* ucmp) {
  SmallestKeyHeap smallest_key_priority_q =
      SmallestKeyHeap(SmallestKeyHeapComparator(ucmp));

  InputFileInfo input_file;

  for (size_t l = 0; l < c->num_input_levels(); l++) {
    if (c->num_input_files(l) != 0) {
      if (l == 0 && c->start_level() == 0) {
        for (size_t i = 0; i < c->num_input_files(0); i++) {
          input_file.f = c->input(0, i);
          input_file.level = 0;
          input_file.index = i;
          smallest_key_priority_q.push(std::move(input_file));
        }
      } else {
        input_file.f = c->input(l, 0);
        input_file.level = l;
        input_file.index = 0;
        smallest_key_priority_q.push(std::move(input_file));
      }
    }
  }
  return smallest_key_priority_q;
}

#ifndef NDEBUG
// smallest_seqno and largest_seqno are set iff. `files` is not empty.
void GetSmallestLargestSeqno(const std::vector<FileMetaData*>& files,
                             SequenceNumber* smallest_seqno,
                             SequenceNumber* largest_seqno) {
  bool is_first = true;
  for (FileMetaData* f : files) {
    assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
    if (is_first) {
      is_first = false;
      *smallest_seqno = f->fd.smallest_seqno;
      *largest_seqno = f->fd.largest_seqno;
    } else {
      if (f->fd.smallest_seqno < *smallest_seqno) {
        *smallest_seqno = f->fd.smallest_seqno;
      }
      if (f->fd.largest_seqno > *largest_seqno) {
        *largest_seqno = f->fd.largest_seqno;
      }
    }
  }
}
#endif
}  // namespace

// Algorithm that checks to see if there are any overlapping
// files in the input
bool UniversalCompactionPicker::IsInputFilesNonOverlapping(Compaction* c) {
  auto comparator = icmp_->user_comparator();
  int first_iter = 1;

  InputFileInfo prev, curr, next;

  SmallestKeyHeap smallest_key_priority_q =
      create_level_heap(c, icmp_->user_comparator());

  while (!smallest_key_priority_q.empty()) {
    curr = smallest_key_priority_q.top();
    smallest_key_priority_q.pop();

    if (first_iter) {
      prev = curr;
      first_iter = 0;
    } else {
      if (comparator->Compare(prev.f->largest.user_key(),
                              curr.f->smallest.user_key()) >= 0) {
        // found overlapping files, return false
        return false;
      }
      assert(comparator->Compare(curr.f->largest.user_key(),
                                 prev.f->largest.user_key()) > 0);
      prev = curr;
    }

    next.f = nullptr;

    if (curr.level != 0 && curr.index < c->num_input_files(curr.level) - 1) {
      next.f = c->input(curr.level, curr.index + 1);
      next.level = curr.level;
      next.index = curr.index + 1;
    }

    if (next.f) {
      smallest_key_priority_q.push(std::move(next));
    }
  }
  return true;
}

bool UniversalCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  const int kLevel0 = 0;
  if (vstorage->CompactionScore(kLevel0) >= 1) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  return false;
}

void UniversalCompactionPicker::SortedRun::Dump(char* out_buf,
                                                size_t out_buf_size,
                                                bool print_path) const {
  if (level == 0) {
    assert(file != nullptr);
    if (file->fd.GetPathId() == 0 || !print_path) {
      snprintf(out_buf, out_buf_size, "file %" PRIu64, file->fd.GetNumber());
    } else {
      snprintf(out_buf, out_buf_size, "file %" PRIu64
                                      "(path "
                                      "%" PRIu32 ")",
               file->fd.GetNumber(), file->fd.GetPathId());
    }
  } else {
    snprintf(out_buf, out_buf_size, "level %d", level);
  }
}

void UniversalCompactionPicker::SortedRun::DumpSizeInfo(
    char* out_buf, size_t out_buf_size, size_t sorted_run_count) const {
  if (level == 0) {
    assert(file != nullptr);
    snprintf(out_buf, out_buf_size,
             "file %" PRIu64 "[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             file->fd.GetNumber(), sorted_run_count, file->fd.GetFileSize(),
             file->compensated_file_size);
  } else {
    snprintf(out_buf, out_buf_size,
             "level %d[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             level, sorted_run_count, size, compensated_file_size);
  }
}

std::vector<UniversalCompactionPicker::SortedRun>
UniversalCompactionPicker::CalculateSortedRuns(
    const VersionStorageInfo& vstorage, const ImmutableCFOptions& /*ioptions*/,
    const MutableCFOptions& mutable_cf_options) {
  std::vector<UniversalCompactionPicker::SortedRun> ret;
  for (FileMetaData* f : vstorage.LevelFiles(0)) {
    ret.emplace_back(0, f, f->fd.GetFileSize(), f->compensated_file_size,
                     f->being_compacted);
  }
  for (int level = 1; level < vstorage.num_levels(); level++) {
    uint64_t total_compensated_size = 0U;
    uint64_t total_size = 0U;
    bool being_compacted = false;
    bool is_first = true;
    for (FileMetaData* f : vstorage.LevelFiles(level)) {
      total_compensated_size += f->compensated_file_size;
      total_size += f->fd.GetFileSize();
      if (mutable_cf_options.compaction_options_universal.allow_trivial_move ==
          true) {
        if (f->being_compacted) {
          being_compacted = f->being_compacted;
        }
      } else {
        // Compaction always includes all files for a non-zero level, so for a
        // non-zero level, all the files should share the same being_compacted
        // value.
        // This assumption is only valid when
        // mutable_cf_options.compaction_options_universal.allow_trivial_move is
        // false
        assert(is_first || f->being_compacted == being_compacted);
      }
      if (is_first) {
        being_compacted = f->being_compacted;
        is_first = false;
      }
    }
    if (total_compensated_size > 0) {
      ret.emplace_back(level, nullptr, total_size, total_compensated_size,
                       being_compacted);
    }
  }
  return ret;
}

// Universal style of compaction. Pick files that are contiguous in
// time-range to compact.
Compaction* UniversalCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  const int kLevel0 = 0;
  double score = vstorage->CompactionScore(kLevel0);
  std::vector<SortedRun> sorted_runs =
      CalculateSortedRuns(*vstorage, ioptions_, mutable_cf_options);

  if (sorted_runs.size() == 0 ||
      (vstorage->FilesMarkedForCompaction().empty() &&
       sorted_runs.size() < (unsigned int)mutable_cf_options
                                .level0_file_num_compaction_trigger)) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: nothing to do\n",
                     cf_name.c_str());
    TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                             nullptr);
    return nullptr;
  }
  VersionStorageInfo::LevelSummaryStorage tmp;
  ROCKS_LOG_BUFFER_MAX_SZ(
      log_buffer, 3072,
      "[%s] Universal: sorted runs files(%" ROCKSDB_PRIszt "): %s\n",
      cf_name.c_str(), sorted_runs.size(), vstorage->LevelSummary(&tmp));

  // Check for size amplification first.
  Compaction* c = nullptr;
  if (sorted_runs.size() >=
      static_cast<size_t>(
          mutable_cf_options.level0_file_num_compaction_trigger)) {
    if ((c = PickCompactionToReduceSizeAmp(cf_name, mutable_cf_options,
                                           vstorage, score, sorted_runs,
                                           log_buffer)) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: compacting for size amp\n",
                       cf_name.c_str());
    } else {
      // Size amplification is within limits. Try reducing read
      // amplification while maintaining file size ratios.
      unsigned int ratio =
          mutable_cf_options.compaction_options_universal.size_ratio;

      if ((c = PickCompactionToReduceSortedRuns(
               cf_name, mutable_cf_options, vstorage, score, ratio, UINT_MAX,
               sorted_runs, log_buffer)) != nullptr) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Universal: compacting for size ratio\n",
                         cf_name.c_str());
      } else {
        // Size amplification and file size ratios are within configured limits.
        // If max read amplification is exceeding configured limits, then force
        // compaction without looking at filesize ratios and try to reduce
        // the number of files to fewer than level0_file_num_compaction_trigger.
        // This is guaranteed by NeedsCompaction()
        assert(sorted_runs.size() >=
               static_cast<size_t>(
                   mutable_cf_options.level0_file_num_compaction_trigger));
        // Get the total number of sorted runs that are not being compacted
        int num_sr_not_compacted = 0;
        for (size_t i = 0; i < sorted_runs.size(); i++) {
          if (sorted_runs[i].being_compacted == false) {
            num_sr_not_compacted++;
          }
        }

        // The number of sorted runs that are not being compacted is greater
        // than the maximum allowed number of sorted runs
        if (num_sr_not_compacted >
            mutable_cf_options.level0_file_num_compaction_trigger) {
          unsigned int num_files =
              num_sr_not_compacted -
              mutable_cf_options.level0_file_num_compaction_trigger + 1;
          if ((c = PickCompactionToReduceSortedRuns(
                   cf_name, mutable_cf_options, vstorage, score, UINT_MAX,
                   num_files, sorted_runs, log_buffer)) != nullptr) {
            ROCKS_LOG_BUFFER(log_buffer,
                             "[%s] Universal: compacting for file num -- %u\n",
                             cf_name.c_str(), num_files);
          }
        }
      }
    }
  }

  if (c == nullptr) {
    if ((c = PickDeleteTriggeredCompaction(cf_name, mutable_cf_options,
                                           vstorage, score, sorted_runs,
                                           log_buffer)) != nullptr) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Universal: delete triggered compaction\n",
                       cf_name.c_str());
    }
  }

  if (c == nullptr) {
    TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                             nullptr);
    return nullptr;
  }

  if (mutable_cf_options.compaction_options_universal.allow_trivial_move ==
      true) {
    c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
  }

// validate that all the chosen files of L0 are non overlapping in time
#ifndef NDEBUG
  SequenceNumber prev_smallest_seqno = 0U;
  bool is_first = true;

  size_t level_index = 0U;
  if (c->start_level() == 0) {
    for (auto f : *c->inputs(0)) {
      assert(f->fd.smallest_seqno <= f->fd.largest_seqno);
      if (is_first) {
        is_first = false;
      }
      prev_smallest_seqno = f->fd.smallest_seqno;
    }
    level_index = 1U;
  }
  for (; level_index < c->num_input_levels(); level_index++) {
    if (c->num_input_files(level_index) != 0) {
      SequenceNumber smallest_seqno = 0U;
      SequenceNumber largest_seqno = 0U;
      GetSmallestLargestSeqno(*(c->inputs(level_index)), &smallest_seqno,
                              &largest_seqno);
      if (is_first) {
        is_first = false;
      } else if (prev_smallest_seqno > 0) {
        // A level is considered as the bottommost level if there are
        // no files in higher levels or if files in higher levels do
        // not overlap with the files being compacted. Sequence numbers
        // of files in bottommost level can be set to 0 to help
        // compression. As a result, the following assert may not hold
        // if the prev_smallest_seqno is 0.
        assert(prev_smallest_seqno > largest_seqno);
      }
      prev_smallest_seqno = smallest_seqno;
    }
  }
#endif
  // update statistics
  MeasureTime(ioptions_.statistics, NUM_FILES_IN_SINGLE_COMPACTION,
              c->inputs(0)->size());

  RegisterCompaction(c);
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);

  TEST_SYNC_POINT_CALLBACK("UniversalCompactionPicker::PickCompaction:Return",
                           c);
  return c;
}

uint32_t UniversalCompactionPicker::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, uint64_t file_size) {
  // Two conditions need to be satisfied:
  // (1) the target path needs to be able to hold the file's size
  // (2) Total size left in this and previous paths need to be not
  //     smaller than expected future file size before this new file is
  //     compacted, which is estimated based on size_ratio.
  // For example, if now we are compacting files of size (1, 1, 2, 4, 8),
  // we will make sure the target file, probably with size of 16, will be
  // placed in a path so that eventually when new files are generated and
  // compacted to (1, 1, 2, 4, 8, 16), all those files can be stored in or
  // before the path we chose.
  //
  // TODO(sdong): now the case of multiple column families is not
  // considered in this algorithm. So the target size can be violated in
  // that case. We need to improve it.
  uint64_t accumulated_size = 0;
  uint64_t future_size =
      file_size *
      (100 - mutable_cf_options.compaction_options_universal.size_ratio) / 100;
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());
  for (; p < ioptions.cf_paths.size() - 1; p++) {
    uint64_t target_size = ioptions.cf_paths[p].target_size;
    if (target_size > file_size &&
        accumulated_size + (target_size - file_size) > future_size) {
      return p;
    }
    accumulated_size += target_size;
  }
  return p;
}

//
// Consider compaction files based on their size differences with
// the next file in time order.
//
Compaction* UniversalCompactionPicker::PickCompactionToReduceSortedRuns(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score, unsigned int ratio,
    unsigned int max_number_of_files_to_compact,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  unsigned int min_merge_width =
      mutable_cf_options.compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
      mutable_cf_options.compaction_options_universal.max_merge_width;

  const SortedRun* sr = nullptr;
  bool done = false;
  size_t start_index = 0;
  unsigned int candidate_count = 0;

  unsigned int max_files_to_compact =
      std::min(max_merge_width, max_number_of_files_to_compact);
  min_merge_width = std::max(min_merge_width, 2U);

  // Caller checks the size before executing this function. This invariant is
  // important because otherwise we may have a possible integer underflow when
  // dealing with unsigned types.
  assert(sorted_runs.size() > 0);

  // Considers a candidate file only if it is smaller than the
  // total size accumulated so far.
  for (size_t loop = 0; loop < sorted_runs.size(); loop++) {
    candidate_count = 0;

    // Skip files that are already being compacted
    for (sr = nullptr; loop < sorted_runs.size(); loop++) {
      sr = &sorted_runs[loop];

      if (!sr->being_compacted) {
        candidate_count = 1;
        break;
      }
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf));
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Universal: %s"
                       "[%d] being compacted, skipping",
                       cf_name.c_str(), file_num_buf, loop);

      sr = nullptr;
    }

    // This file is not being compacted. Consider it as the
    // first candidate to be compacted.
    uint64_t candidate_size = sr != nullptr ? sr->compensated_file_size : 0;
    if (sr != nullptr) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Possible candidate %s[%d].",
                       cf_name.c_str(), file_num_buf, loop);
    }

    // Check if the succeeding files need compaction.
    for (size_t i = loop + 1;
         candidate_count < max_files_to_compact && i < sorted_runs.size();
         i++) {
      const SortedRun* succeeding_sr = &sorted_runs[i];
      if (succeeding_sr->being_compacted) {
        break;
      }
      // Pick files if the total/last candidate file size (increased by the
      // specified ratio) is still larger than the next candidate file.
      // candidate_size is the total size of files picked so far with the
      // default kCompactionStopStyleTotalSize; with
      // kCompactionStopStyleSimilarSize, it's simply the size of the last
      // picked file.
      double sz = candidate_size * (100.0 + ratio) / 100.0;
      if (sz < static_cast<double>(succeeding_sr->size)) {
        break;
      }
      if (mutable_cf_options.compaction_options_universal.stop_style ==
          kCompactionStopStyleSimilarSize) {
        // Similar-size stopping rule: also check the last picked file isn't
        // far larger than the next candidate file.
        sz = (succeeding_sr->size * (100.0 + ratio)) / 100.0;
        if (sz < static_cast<double>(candidate_size)) {
          // If the small file we've encountered begins a run of similar-size
          // files, we'll pick them up on a future iteration of the outer
          // loop. If it's some lonely straggler, it'll eventually get picked
          // by the last-resort read amp strategy which disregards size ratios.
          break;
        }
        candidate_size = succeeding_sr->compensated_file_size;
      } else {  // default kCompactionStopStyleTotalSize
        candidate_size += succeeding_sr->compensated_file_size;
      }
      candidate_count++;
    }

    // Found a series of consecutive files that need compaction.
    if (candidate_count >= (unsigned int)min_merge_width) {
      start_index = loop;
      done = true;
      break;
    } else {
      for (size_t i = loop;
           i < loop + candidate_count && i < sorted_runs.size(); i++) {
        const SortedRun* skipping_sr = &sorted_runs[i];
        char file_num_buf[256];
        skipping_sr->DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
        ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Skipping %s",
                         cf_name.c_str(), file_num_buf);
      }
    }
  }
  if (!done || candidate_count <= 1) {
    return nullptr;
  }
  size_t first_index_after = start_index + candidate_count;
  // Compression is enabled if files compacted earlier already reached
  // size ratio of compression.
  bool enable_compression = true;
  int ratio_to_compress =
      mutable_cf_options.compaction_options_universal.compression_size_percent;
  if (ratio_to_compress >= 0) {
    uint64_t total_size = 0;
    for (auto& sorted_run : sorted_runs) {
      total_size += sorted_run.compensated_file_size;
    }

    uint64_t older_file_size = 0;
    for (size_t i = sorted_runs.size() - 1; i >= first_index_after; i--) {
      older_file_size += sorted_runs[i].size;
      if (older_file_size * 100L >= total_size * (long)ratio_to_compress) {
        enable_compression = false;
        break;
      }
    }
  }

  uint64_t estimated_total_size = 0;
  for (unsigned int i = 0; i < first_index_after; i++) {
    estimated_total_size += sorted_runs[i].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options, estimated_total_size);
  int start_level = sorted_runs[start_index].level;
  int output_level;
  if (first_index_after == sorted_runs.size()) {
    output_level = vstorage->num_levels() - 1;
  } else if (sorted_runs[first_index_after].level == 0) {
    output_level = 0;
  } else {
    output_level = sorted_runs[first_index_after].level - 1;
  }

  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind &&
      (output_level == vstorage->num_levels() - 1)) {
    assert(output_level > 1);
    output_level--;
  }

  std::vector<CompactionInputFiles> inputs(vstorage->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t i = start_index; i < first_index_after; i++) {
    auto& picking_sr = sorted_runs[i];
    if (picking_sr.level == 0) {
      FileMetaData* picking_file = picking_sr.file;
      inputs[0].files.push_back(picking_file);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), i);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Picking %s", cf_name.c_str(),
                     file_num_buf);
  }

  CompactionReason compaction_reason;
  if (max_number_of_files_to_compact == UINT_MAX) {
    compaction_reason = CompactionReason::kUniversalSizeRatio;
  } else {
    compaction_reason = CompactionReason::kUniversalSortedRunNum;
  }
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, start_level,
                         1, enable_compression),
      GetCompressionOptions(ioptions_, vstorage, start_level,
                            enable_compression),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score, false /* deletion_compaction */, compaction_reason);
}

// Look at overall size amplification. If size amplification
// exceeeds the configured value, then do a compaction
// of the candidate files all the way upto the earliest
// base file (overrides configured values of file-size ratios,
// min_merge_width and max_merge_width).
//
Compaction* UniversalCompactionPicker::PickCompactionToReduceSizeAmp(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  // percentage flexibility while reducing size amplification
  uint64_t ratio = mutable_cf_options.compaction_options_universal
                       .max_size_amplification_percent;

  unsigned int candidate_count = 0;
  uint64_t candidate_size = 0;
  size_t start_index = 0;
  const SortedRun* sr = nullptr;

  if (sorted_runs.back().being_compacted) {
    return nullptr;
  }

  // Skip files that are already being compacted
  for (size_t loop = 0; loop < sorted_runs.size() - 1; loop++) {
    sr = &sorted_runs[loop];
    if (!sr->being_compacted) {
      start_index = loop;  // Consider this as the first candidate.
      break;
    }
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: skipping %s[%d] compacted %s",
                     cf_name.c_str(), file_num_buf, loop,
                     " cannot be a candidate to reduce size amp.\n");
    sr = nullptr;
  }

  if (sr == nullptr) {
    return nullptr;  // no candidate files
  }
  {
    char file_num_buf[kFormatFileNumberBufSize];
    sr->Dump(file_num_buf, sizeof(file_num_buf), true);
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: First candidate %s[%" ROCKSDB_PRIszt "] %s",
        cf_name.c_str(), file_num_buf, start_index, " to reduce size amp.\n");
  }

  // keep adding up all the remaining files
  for (size_t loop = start_index; loop < sorted_runs.size() - 1; loop++) {
    sr = &sorted_runs[loop];
    if (sr->being_compacted) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(
          log_buffer, "[%s] Universal: Possible candidate %s[%d] %s",
          cf_name.c_str(), file_num_buf, start_index,
          " is already being compacted. No size amp reduction possible.\n");
      return nullptr;
    }
    candidate_size += sr->compensated_file_size;
    candidate_count++;
  }
  if (candidate_count == 0) {
    return nullptr;
  }

  // size of earliest file
  uint64_t earliest_file_size = sorted_runs.back().size;

  // size amplification = percentage of additional size
  if (candidate_size * 100 < ratio * earliest_file_size) {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: size amp not needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name.c_str(), candidate_size, earliest_file_size);
    return nullptr;
  } else {
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Universal: size amp needed. newer-files-total-size %" PRIu64
        " earliest-file-size %" PRIu64,
        cf_name.c_str(), candidate_size, earliest_file_size);
  }
  assert(start_index < sorted_runs.size() - 1);

  // Estimate total file size
  uint64_t estimated_total_size = 0;
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    estimated_total_size += sorted_runs[loop].size;
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options, estimated_total_size);
  int start_level = sorted_runs[start_index].level;

  std::vector<CompactionInputFiles> inputs(vstorage->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  // We always compact all the files, so always compress.
  for (size_t loop = start_index; loop < sorted_runs.size(); loop++) {
    auto& picking_sr = sorted_runs[loop];
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: size amp picking %s",
                     cf_name.c_str(), file_num_buf);
  }

  // output files at the bottom most level, unless it's reserved
  int output_level = vstorage->num_levels() - 1;
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind) {
    assert(output_level > 1);
    output_level--;
  }

  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, output_level,
                         1),
      GetCompressionOptions(ioptions_, vstorage, output_level),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score, false /* deletion_compaction */,
      CompactionReason::kUniversalSizeAmplification);
}

// Pick files marked for compaction. Typically, files are marked by
// CompactOnDeleteCollector due to the presence of tombstones.
Compaction* UniversalCompactionPicker::PickDeleteTriggeredCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, double score,
    const std::vector<SortedRun>& /*sorted_runs*/, LogBuffer* /*log_buffer*/) {
  CompactionInputFiles start_level_inputs;
  int output_level;
  std::vector<CompactionInputFiles> inputs;

  if (vstorage->num_levels() == 1) {
    // This is single level universal. Since we're basically trying to reclaim
    // space by processing files marked for compaction due to high tombstone
    // density, let's do the same thing as compaction to reduce size amp which
    // has the same goals.
    bool compact = false;

    start_level_inputs.level = 0;
    start_level_inputs.files.clear();
    output_level = 0;
    for (FileMetaData* f : vstorage->LevelFiles(0)) {
      if (f->marked_for_compaction) {
        compact = true;
      }
      if (compact) {
        start_level_inputs.files.push_back(f);
      }
    }
    if (start_level_inputs.size() <= 1) {
      // If only the last file in L0 is marked for compaction, ignore it
      return nullptr;
    }
    inputs.push_back(start_level_inputs);
  } else {
    int start_level;

    // For multi-level universal, the strategy is to make this look more like
    // leveled. We pick one of the files marked for compaction and compact with
    // overlapping files in the adjacent level.
    PickFilesMarkedForCompaction(cf_name, vstorage, &start_level, &output_level,
                                 &start_level_inputs);
    if (start_level_inputs.empty()) {
      return nullptr;
    }

    // Pick the first non-empty level after the start_level
    for (output_level = start_level + 1; output_level < vstorage->num_levels();
         output_level++) {
      if (vstorage->NumLevelFiles(output_level) != 0) {
        break;
      }
    }

    // If all higher levels are empty, pick the highest level as output level
    if (output_level == vstorage->num_levels()) {
      if (start_level == 0) {
        output_level = vstorage->num_levels() - 1;
      } else {
        // If start level is non-zero and all higher levels are empty, this
        // compaction will translate into a trivial move. Since the idea is
        // to reclaim space and trivial move doesn't help with that, we
        // skip compaction in this case and return nullptr
        return nullptr;
      }
    }
    if (ioptions_.allow_ingest_behind &&
        output_level == vstorage->num_levels() - 1) {
      assert(output_level > 1);
      output_level--;
    }

    if (output_level != 0) {
      if (start_level == 0) {
        if (!GetOverlappingL0Files(vstorage, &start_level_inputs, output_level,
                                   nullptr)) {
          return nullptr;
        }
      }

      CompactionInputFiles output_level_inputs;
      int parent_index = -1;

      output_level_inputs.level = output_level;
      if (!SetupOtherInputs(cf_name, mutable_cf_options, vstorage,
                            &start_level_inputs, &output_level_inputs,
                            &parent_index, -1)) {
        return nullptr;
      }
      inputs.push_back(start_level_inputs);
      if (!output_level_inputs.empty()) {
        inputs.push_back(output_level_inputs);
      }
      if (FilesRangeOverlapWithCompaction(inputs, output_level)) {
        return nullptr;
      }
    } else {
      inputs.push_back(start_level_inputs);
    }
  }

  uint64_t estimated_total_size = 0;
  // Use size of the output level as estimated file size
  for (FileMetaData* f : vstorage->LevelFiles(output_level)) {
    estimated_total_size += f->fd.GetFileSize();
  }
  uint32_t path_id =
      GetPathId(ioptions_, mutable_cf_options, estimated_total_size);
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, output_level,
                         1),
      GetCompressionOptions(ioptions_, vstorage, output_level),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ true,
      score, false /* deletion_compaction */,
      CompactionReason::kFilesMarkedForCompaction);
}
}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
