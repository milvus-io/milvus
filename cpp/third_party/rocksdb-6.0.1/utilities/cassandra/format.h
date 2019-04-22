// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

/**
 * The encoding of Cassandra Row Value.
 *
 * A Cassandra Row Value could either be a row tombstone,
 * or contains multiple columns, it has following fields:
 *
 * struct row_value {
 *   int32_t local_deletion_time;  // Time in second when the row is deleted,
 *                                 // only used for Cassandra tombstone gc.
 *   int64_t marked_for_delete_at; // Ms that marked this row is deleted.
 *   struct column_base columns[]; // For non tombstone row, all columns
 *                                 // are stored here.
 * }
 *
 * If the local_deletion_time and marked_for_delete_at is set, then this is
 * a tombstone, otherwise it contains multiple columns.
 *
 * There are three type of Columns: Normal Column, Expiring Column and Column
 * Tombstone, which have following fields:
 *
 * // Identify the type of the column.
 * enum mask {
 *   DELETION_MASK = 0x01,
 *   EXPIRATION_MASK = 0x02,
 * };
 *
 * struct column  {
 *   int8_t mask = 0;
 *   int8_t index;
 *   int64_t timestamp;
 *   int32_t value_length;
 *   char value[value_length];
 * }
 *
 * struct expiring_column  {
 *   int8_t mask = mask.EXPIRATION_MASK;
 *   int8_t index;
 *   int64_t timestamp;
 *   int32_t value_length;
 *   char value[value_length];
 *   int32_t ttl;
 * }
 *
 * struct tombstone_column  {
 *   int8_t mask = mask.DELETION_MASK;
 *   int8_t index;
 *   int32_t local_deletion_time; // Similar to row_value's field.
 *   int64_t marked_for_delete_at;
 *  }
 */

#pragma once
#include <chrono>
#include <vector>
#include <memory>
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/testharness.h"

namespace rocksdb {
namespace cassandra {

// Identify the type of the column.
enum ColumnTypeMask {
  DELETION_MASK = 0x01,
  EXPIRATION_MASK = 0x02,
};


class ColumnBase {
public:
  ColumnBase(int8_t mask, int8_t index);
  virtual ~ColumnBase() = default;

  virtual int64_t Timestamp() const = 0;
  virtual int8_t Mask() const;
  virtual int8_t Index() const;
  virtual std::size_t Size() const;
  virtual void Serialize(std::string* dest) const;
  static std::shared_ptr<ColumnBase> Deserialize(const char* src,
                                                 std::size_t offset);

private:
  int8_t mask_;
  int8_t index_;
};

class Column : public ColumnBase {
public:
  Column(int8_t mask, int8_t index, int64_t timestamp,
    int32_t value_size, const char* value);

  virtual int64_t Timestamp() const override;
  virtual std::size_t Size() const override;
  virtual void Serialize(std::string* dest) const override;
  static std::shared_ptr<Column> Deserialize(const char* src,
                                             std::size_t offset);

private:
  int64_t timestamp_;
  int32_t value_size_;
  const char* value_;
};

class Tombstone : public ColumnBase {
public:
  Tombstone(int8_t mask, int8_t index,
    int32_t local_deletion_time, int64_t marked_for_delete_at);

  virtual int64_t Timestamp() const override;
  virtual std::size_t Size() const override;
  virtual void Serialize(std::string* dest) const override;
  bool Collectable(int32_t gc_grace_period) const;
  static std::shared_ptr<Tombstone> Deserialize(const char* src,
                                                std::size_t offset);

private:
  int32_t local_deletion_time_;
  int64_t marked_for_delete_at_;
};

class ExpiringColumn : public Column {
public:
  ExpiringColumn(int8_t mask, int8_t index, int64_t timestamp,
    int32_t value_size, const char* value, int32_t ttl);

  virtual std::size_t Size() const override;
  virtual void Serialize(std::string* dest) const override;
  bool Expired() const;
  std::shared_ptr<Tombstone> ToTombstone() const;

  static std::shared_ptr<ExpiringColumn> Deserialize(const char* src,
                                                     std::size_t offset);

private:
  int32_t ttl_;
  std::chrono::time_point<std::chrono::system_clock> TimePoint() const;
  std::chrono::seconds Ttl() const;
};

typedef std::vector<std::shared_ptr<ColumnBase>> Columns;

class RowValue {
public:
  // Create a Row Tombstone.
  RowValue(int32_t local_deletion_time, int64_t marked_for_delete_at);
  // Create a Row containing columns.
  RowValue(Columns columns,
           int64_t last_modified_time);
  RowValue(const RowValue& /*that*/) = delete;
  RowValue(RowValue&& /*that*/) noexcept = default;
  RowValue& operator=(const RowValue& /*that*/) = delete;
  RowValue& operator=(RowValue&& /*that*/) = default;

  std::size_t Size() const;;
  bool IsTombstone() const;
  // For Tombstone this returns the marked_for_delete_at_,
  // otherwise it returns the max timestamp of containing columns.
  int64_t LastModifiedTime() const;
  void Serialize(std::string* dest) const;
  RowValue RemoveExpiredColumns(bool* changed) const;
  RowValue ConvertExpiredColumnsToTombstones(bool* changed) const;
  RowValue RemoveTombstones(int32_t gc_grace_period) const;
  bool Empty() const;

  static RowValue Deserialize(const char* src, std::size_t size);
  // Merge multiple rows according to their timestamp.
  static RowValue Merge(std::vector<RowValue>&& values);

private:
  int32_t local_deletion_time_;
  int64_t marked_for_delete_at_;
  Columns columns_;
  int64_t last_modified_time_;

  FRIEND_TEST(RowValueTest, PurgeTtlShouldRemvoeAllColumnsExpired);
  FRIEND_TEST(RowValueTest, ExpireTtlShouldConvertExpiredColumnsToTombstones);
  FRIEND_TEST(RowValueMergeTest, Merge);
  FRIEND_TEST(RowValueMergeTest, MergeWithRowTombstone);
  FRIEND_TEST(CassandraFunctionalTest, SimpleMergeTest);
  FRIEND_TEST(
    CassandraFunctionalTest, CompactionShouldConvertExpiredColumnsToTombstone);
  FRIEND_TEST(
    CassandraFunctionalTest, CompactionShouldPurgeExpiredColumnsIfPurgeTtlIsOn);
  FRIEND_TEST(
    CassandraFunctionalTest, CompactionShouldRemoveRowWhenAllColumnExpiredIfPurgeTtlIsOn);
  FRIEND_TEST(CassandraFunctionalTest,
              CompactionShouldRemoveTombstoneExceedingGCGracePeriod);
};

} // namepsace cassandrda
} // namespace rocksdb
