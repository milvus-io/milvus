//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/sst_dump_tool_imp.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"

#include "port/port.h"

namespace rocksdb {

SstFileDumper::SstFileDumper(const Options& options,
                             const std::string& file_path, bool verify_checksum,
                             bool output_hex)
    : file_name_(file_path),
      read_num_(0),
      verify_checksum_(verify_checksum),
      output_hex_(output_hex),
      options_(options),
      ioptions_(options_),
      moptions_(ColumnFamilyOptions(options_)),
      internal_comparator_(BytewiseComparator()) {
  fprintf(stdout, "Process %s\n", file_path.c_str());
  init_result_ = GetTableReader(file_name_);
}

extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;

const char* testFileName = "test_file_name";

static const std::vector<std::pair<CompressionType, const char*>>
    kCompressions = {
        {CompressionType::kNoCompression, "kNoCompression"},
        {CompressionType::kSnappyCompression, "kSnappyCompression"},
        {CompressionType::kZlibCompression, "kZlibCompression"},
        {CompressionType::kBZip2Compression, "kBZip2Compression"},
        {CompressionType::kLZ4Compression, "kLZ4Compression"},
        {CompressionType::kLZ4HCCompression, "kLZ4HCCompression"},
        {CompressionType::kXpressCompression, "kXpressCompression"},
        {CompressionType::kZSTD, "kZSTD"}};

Status SstFileDumper::GetTableReader(const std::string& file_path) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kInvalidTableMagicNumber;

  // read table magic number
  Footer footer;

  std::unique_ptr<RandomAccessFile> file;
  uint64_t file_size = 0;
  Status s = options_.env->NewRandomAccessFile(file_path, &file, soptions_);
  if (s.ok()) {
    s = options_.env->GetFileSize(file_path, &file_size);
  }

  file_.reset(new RandomAccessFileReader(std::move(file), file_path));

  if (s.ok()) {
    s = ReadFooterFromFile(file_.get(), nullptr /* prefetch_buffer */,
                           file_size, &footer);
  }
  if (s.ok()) {
    magic_number = footer.table_magic_number();
  }

  if (s.ok()) {
    if (magic_number == kPlainTableMagicNumber ||
        magic_number == kLegacyPlainTableMagicNumber) {
      soptions_.use_mmap_reads = true;
      options_.env->NewRandomAccessFile(file_path, &file, soptions_);
      file_.reset(new RandomAccessFileReader(std::move(file), file_path));
    }
    options_.comparator = &internal_comparator_;
    // For old sst format, ReadTableProperties might fail but file can be read
    if (ReadTableProperties(magic_number, file_.get(), file_size).ok()) {
      SetTableOptionsByMagicNumber(magic_number);
    } else {
      SetOldTableOptions();
    }
  }

  if (s.ok()) {
    s = NewTableReader(ioptions_, soptions_, internal_comparator_, file_size,
                       &table_reader_);
  }
  return s;
}

Status SstFileDumper::NewTableReader(
    const ImmutableCFOptions& /*ioptions*/, const EnvOptions& /*soptions*/,
    const InternalKeyComparator& /*internal_comparator*/, uint64_t file_size,
    std::unique_ptr<TableReader>* /*table_reader*/) {
  // We need to turn off pre-fetching of index and filter nodes for
  // BlockBasedTable
  if (BlockBasedTableFactory::kName == options_.table_factory->Name()) {
    return options_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, moptions_.prefix_extractor.get(),
                           soptions_, internal_comparator_),
        std::move(file_), file_size, &table_reader_, /*enable_prefetch=*/false);
  }

  // For all other factory implementation
  return options_.table_factory->NewTableReader(
      TableReaderOptions(ioptions_, moptions_.prefix_extractor.get(), soptions_,
                         internal_comparator_),
      std::move(file_), file_size, &table_reader_);
}

Status SstFileDumper::VerifyChecksum() {
  return table_reader_->VerifyChecksum();
}

Status SstFileDumper::DumpTable(const std::string& out_filename) {
  std::unique_ptr<WritableFile> out_file;
  Env* env = Env::Default();
  env->NewWritableFile(out_filename, &out_file, soptions_);
  Status s = table_reader_->DumpTable(out_file.get(),
                                      moptions_.prefix_extractor.get());
  out_file->Close();
  return s;
}

uint64_t SstFileDumper::CalculateCompressedTableSize(
    const TableBuilderOptions& tb_options, size_t block_size) {
  std::unique_ptr<WritableFile> out_file;
  std::unique_ptr<Env> env(NewMemEnv(Env::Default()));
  env->NewWritableFile(testFileName, &out_file, soptions_);
  std::unique_ptr<WritableFileWriter> dest_writer;
  dest_writer.reset(
      new WritableFileWriter(std::move(out_file), testFileName, soptions_));
  BlockBasedTableOptions table_options;
  table_options.block_size = block_size;
  BlockBasedTableFactory block_based_tf(table_options);
  std::unique_ptr<TableBuilder> table_builder;
  table_builder.reset(block_based_tf.NewTableBuilder(
      tb_options,
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      dest_writer.get()));
  std::unique_ptr<InternalIterator> iter(table_reader_->NewIterator(
      ReadOptions(), moptions_.prefix_extractor.get()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!iter->status().ok()) {
      fputs(iter->status().ToString().c_str(), stderr);
      exit(1);
    }
    table_builder->Add(iter->key(), iter->value());
  }
  Status s = table_builder->Finish();
  if (!s.ok()) {
    fputs(s.ToString().c_str(), stderr);
    exit(1);
  }
  uint64_t size = table_builder->FileSize();
  env->DeleteFile(testFileName);
  return size;
}

int SstFileDumper::ShowAllCompressionSizes(
    size_t block_size,
    const std::vector<std::pair<CompressionType, const char*>>&
        compression_types) {
  ReadOptions read_options;
  Options opts;
  const ImmutableCFOptions imoptions(opts);
  const ColumnFamilyOptions cfo(opts);
  const MutableCFOptions moptions(cfo);
  rocksdb::InternalKeyComparator ikc(opts.comparator);
  std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
      block_based_table_factories;

  fprintf(stdout, "Block Size: %" ROCKSDB_PRIszt "\n", block_size);

  for (auto& i : compression_types) {
    if (CompressionTypeSupported(i.first)) {
      CompressionOptions compress_opt;
      std::string column_family_name;
      int unknown_level = -1;
      TableBuilderOptions tb_opts(imoptions, moptions, ikc,
                                  &block_based_table_factories, i.first,
                                  compress_opt, false /* skip_filters */,
                                  column_family_name, unknown_level);
      uint64_t file_size = CalculateCompressedTableSize(tb_opts, block_size);
      fprintf(stdout, "Compression: %s", i.second);
      fprintf(stdout, " Size: %" PRIu64 "\n", file_size);
    } else {
      fprintf(stdout, "Unsupported compression type: %s.\n", i.second);
    }
  }
  return 0;
}

Status SstFileDumper::ReadTableProperties(uint64_t table_magic_number,
                                          RandomAccessFileReader* file,
                                          uint64_t file_size) {
  TableProperties* table_properties = nullptr;
  Status s = rocksdb::ReadTableProperties(file, file_size, table_magic_number,
                                          ioptions_, &table_properties);
  if (s.ok()) {
    table_properties_.reset(table_properties);
  } else {
    fprintf(stdout, "Not able to read table properties\n");
  }
  return s;
}

Status SstFileDumper::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  assert(table_properties_);
  if (table_magic_number == kBlockBasedTableMagicNumber ||
      table_magic_number == kLegacyBlockBasedTableMagicNumber) {
    options_.table_factory = std::make_shared<BlockBasedTableFactory>();
    fprintf(stdout, "Sst file format: block-based\n");
    auto& props = table_properties_->user_collected_properties;
    auto pos = props.find(BlockBasedTablePropertyNames::kIndexType);
    if (pos != props.end()) {
      auto index_type_on_file = static_cast<BlockBasedTableOptions::IndexType>(
          DecodeFixed32(pos->second.c_str()));
      if (index_type_on_file ==
          BlockBasedTableOptions::IndexType::kHashSearch) {
        options_.prefix_extractor.reset(NewNoopTransform());
      }
    }
  } else if (table_magic_number == kPlainTableMagicNumber ||
             table_magic_number == kLegacyPlainTableMagicNumber) {
    options_.allow_mmap_reads = true;

    PlainTableOptions plain_table_options;
    plain_table_options.user_key_len = kPlainTableVariableLength;
    plain_table_options.bloom_bits_per_key = 0;
    plain_table_options.hash_table_ratio = 0;
    plain_table_options.index_sparseness = 1;
    plain_table_options.huge_page_tlb_size = 0;
    plain_table_options.encoding_type = kPlain;
    plain_table_options.full_scan_mode = true;

    options_.table_factory.reset(NewPlainTableFactory(plain_table_options));
    fprintf(stdout, "Sst file format: plain table\n");
  } else {
    char error_msg_buffer[80];
    snprintf(error_msg_buffer, sizeof(error_msg_buffer) - 1,
             "Unsupported table magic number --- %lx",
             (long)table_magic_number);
    return Status::InvalidArgument(error_msg_buffer);
  }

  return Status::OK();
}

Status SstFileDumper::SetOldTableOptions() {
  assert(table_properties_ == nullptr);
  options_.table_factory = std::make_shared<BlockBasedTableFactory>();
  fprintf(stdout, "Sst file format: block-based(old version)\n");

  return Status::OK();
}

Status SstFileDumper::ReadSequential(bool print_kv, uint64_t read_num,
                                     bool has_from, const std::string& from_key,
                                     bool has_to, const std::string& to_key,
                                     bool use_from_as_prefix) {
  if (!table_reader_) {
    return init_result_;
  }

  InternalIterator* iter = table_reader_->NewIterator(
      ReadOptions(verify_checksum_, false), moptions_.prefix_extractor.get());
  uint64_t i = 0;
  if (has_from) {
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(from_key);
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }
  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    Slice value = iter->value();
    ++i;
    if (read_num > 0 && i > read_num)
      break;

    ParsedInternalKey ikey;
    if (!ParseInternalKey(key, &ikey)) {
      std::cerr << "Internal Key ["
                << key.ToString(true /* in hex*/)
                << "] parse error!\n";
      continue;
    }

    // the key returned is not prefixed with out 'from' key
    if (use_from_as_prefix && !ikey.user_key.starts_with(from_key)) {
      break;
    }

    // If end marker was specified, we stop before it
    if (has_to && BytewiseComparator()->Compare(ikey.user_key, to_key) >= 0) {
      break;
    }

    if (print_kv) {
      fprintf(stdout, "%s => %s\n",
          ikey.DebugString(output_hex_).c_str(),
          value.ToString(output_hex_).c_str());
    }
  }

  read_num_ += i;

  Status ret = iter->status();
  delete iter;
  return ret;
}

Status SstFileDumper::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}

namespace {

void print_help() {
  fprintf(stderr,
          R"(sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw]
    --file=<data_dir_OR_sst_file>
      Path to SST file or directory containing SST files

    --command=check|scan|raw|verify
        check: Iterate over entries in files but dont print anything except if an error is encounterd (default command)
        scan: Iterate over entries in files and print them to screen
        raw: Dump all the table contents to <file_name>_dump.txt
        verify: Iterate all the blocks in files verifying checksum to detect possible coruption but dont print anything except if a corruption is encountered
        recompress: reports the SST file size if recompressed with different
                    compression types

    --output_hex
      Can be combined with scan command to print the keys and values in Hex

    --from=<user_key>
      Key to start reading from when executing check|scan

    --to=<user_key>
      Key to stop reading at when executing check|scan

    --prefix=<user_key>
      Returns all keys with this prefix when executing check|scan
      Cannot be used in conjunction with --from

    --read_num=<num>
      Maximum number of entries to read when executing check|scan

    --verify_checksum
      Verify file checksum when executing check|scan

    --input_key_hex
      Can be combined with --from and --to to indicate that these values are encoded in Hex

    --show_properties
      Print table properties after iterating over the file when executing
      check|scan|raw

    --set_block_size=<block_size>
      Can be combined with --command=recompress to set the block size that will
      be used when trying different compression algorithms

    --compression_types=<comma-separated list of CompressionType members, e.g.,
      kSnappyCompression>
      Can be combined with --command=recompress to run recompression for this
      list of compression types

    --parse_internal_key=<0xKEY>
      Convenience option to parse an internal key on the command line. Dumps the
      internal key in hex format {'key' @ SN: type}
)");
}

}  // namespace

int SSTDumpTool::Run(int argc, char** argv, Options options) {
  const char* dir_or_file = nullptr;
  uint64_t read_num = std::numeric_limits<uint64_t>::max();
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;
  bool input_key_hex = false;
  bool has_from = false;
  bool has_to = false;
  bool use_from_as_prefix = false;
  bool show_properties = false;
  bool show_summary = false;
  bool set_block_size = false;
  std::string from_key;
  std::string to_key;
  std::string block_size_str;
  size_t block_size = 0;
  std::vector<std::pair<CompressionType, const char*>> compression_types;
  uint64_t total_num_files = 0;
  uint64_t total_num_data_blocks = 0;
  uint64_t total_data_block_size = 0;
  uint64_t total_index_block_size = 0;
  uint64_t total_filter_block_size = 0;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      dir_or_file = argv[i] + 7;
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i],
               "--read_num=%lu%c",
               (unsigned long*)&n, &junk) == 1) {
      read_num = n;
    } else if (strcmp(argv[i], "--verify_checksum") == 0) {
      verify_checksum = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else if (strncmp(argv[i], "--from=", 7) == 0) {
      from_key = argv[i] + 7;
      has_from = true;
    } else if (strncmp(argv[i], "--to=", 5) == 0) {
      to_key = argv[i] + 5;
      has_to = true;
    } else if (strncmp(argv[i], "--prefix=", 9) == 0) {
      from_key = argv[i] + 9;
      use_from_as_prefix = true;
    } else if (strcmp(argv[i], "--show_properties") == 0) {
      show_properties = true;
    } else if (strcmp(argv[i], "--show_summary") == 0) {
      show_summary = true;
    } else if (strncmp(argv[i], "--set_block_size=", 17) == 0) {
      set_block_size = true;
      block_size_str = argv[i] + 17;
      std::istringstream iss(block_size_str);
      iss >> block_size;
      if (iss.fail()) {
        fprintf(stderr, "block size must be numeric\n");
        exit(1);
      }
    } else if (strncmp(argv[i], "--compression_types=", 20) == 0) {
      std::string compression_types_csv = argv[i] + 20;
      std::istringstream iss(compression_types_csv);
      std::string compression_type;
      while (std::getline(iss, compression_type, ',')) {
        auto iter = std::find_if(
            kCompressions.begin(), kCompressions.end(),
            [&compression_type](std::pair<CompressionType, const char*> curr) {
              return curr.second == compression_type;
            });
        if (iter == kCompressions.end()) {
          fprintf(stderr, "%s is not a valid CompressionType\n",
                  compression_type.c_str());
          exit(1);
        }
        compression_types.emplace_back(*iter);
      }
    } else if (strncmp(argv[i], "--parse_internal_key=", 21) == 0) {
      std::string in_key(argv[i] + 21);
      try {
        in_key = rocksdb::LDBCommand::HexToString(in_key);
      } catch (...) {
        std::cerr << "ERROR: Invalid key input '"
          << in_key
          << "' Use 0x{hex representation of internal rocksdb key}" << std::endl;
        return -1;
      }
      Slice sl_key = rocksdb::Slice(in_key);
      ParsedInternalKey ikey;
      int retc = 0;
      if (!ParseInternalKey(sl_key, &ikey)) {
        std::cerr << "Internal Key [" << sl_key.ToString(true /* in hex*/)
                  << "] parse error!\n";
        retc = -1;
      }
      fprintf(stdout, "key=%s\n", ikey.DebugString(true).c_str());
      return retc;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }

  if (use_from_as_prefix && has_from) {
    fprintf(stderr, "Cannot specify --prefix and --from\n\n");
    exit(1);
  }

  if (input_key_hex) {
    if (has_from || use_from_as_prefix) {
      from_key = rocksdb::LDBCommand::HexToString(from_key);
    }
    if (has_to) {
      to_key = rocksdb::LDBCommand::HexToString(to_key);
    }
  }

  if (dir_or_file == nullptr) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help();
    exit(1);
  }

  std::vector<std::string> filenames;
  rocksdb::Env* env = options.env;
  rocksdb::Status st = env->GetChildren(dir_or_file, &filenames);
  bool dir = true;
  if (!st.ok()) {
    filenames.clear();
    filenames.push_back(dir_or_file);
    dir = false;
  }

  fprintf(stdout, "from [%s] to [%s]\n",
      rocksdb::Slice(from_key).ToString(true).c_str(),
      rocksdb::Slice(to_key).ToString(true).c_str());

  uint64_t total_read = 0;
  for (size_t i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      // ignore
      continue;
    }
    if (dir) {
      filename = std::string(dir_or_file) + "/" + filename;
    }

    rocksdb::SstFileDumper dumper(options, filename, verify_checksum,
                                  output_hex);
    if (!dumper.getStatus().ok()) {
      fprintf(stderr, "%s: %s\n", filename.c_str(),
              dumper.getStatus().ToString().c_str());
      continue;
    }

    if (command == "recompress") {
      dumper.ShowAllCompressionSizes(
          set_block_size ? block_size : 16384,
          compression_types.empty() ? kCompressions : compression_types);
      return 0;
    }

    if (command == "raw") {
      std::string out_filename = filename.substr(0, filename.length() - 4);
      out_filename.append("_dump.txt");

      st = dumper.DumpTable(out_filename);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        exit(1);
      } else {
        fprintf(stdout, "raw dump written to file %s\n", &out_filename[0]);
      }
      continue;
    }

    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = dumper.ReadSequential(
          command == "scan", read_num > 0 ? (read_num - total_read) : read_num,
          has_from || use_from_as_prefix, from_key, has_to, to_key,
          use_from_as_prefix);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(),
            st.ToString().c_str());
      }
      total_read += dumper.GetReadNumber();
      if (read_num > 0 && total_read > read_num) {
        break;
      }
    }

    if (command == "verify") {
      st = dumper.VerifyChecksum();
      if (!st.ok()) {
        fprintf(stderr, "%s is corrupted: %s\n", filename.c_str(),
                st.ToString().c_str());
      } else {
        fprintf(stdout, "The file is ok\n");
      }
      continue;
    }

    if (show_properties || show_summary) {
      const rocksdb::TableProperties* table_properties;

      std::shared_ptr<const rocksdb::TableProperties>
          table_properties_from_reader;
      st = dumper.ReadTableProperties(&table_properties_from_reader);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        fprintf(stderr, "Try to use initial table properties\n");
        table_properties = dumper.GetInitTableProperties();
      } else {
        table_properties = table_properties_from_reader.get();
      }
      if (table_properties != nullptr) {
        if (show_properties) {
          fprintf(stdout,
                  "Table Properties:\n"
                  "------------------------------\n"
                  "  %s",
                  table_properties->ToString("\n  ", ": ").c_str());
        }
        total_num_files += 1;
        total_num_data_blocks += table_properties->num_data_blocks;
        total_data_block_size += table_properties->data_size;
        total_index_block_size += table_properties->index_size;
        total_filter_block_size += table_properties->filter_size;
      }
      if (show_properties) {
        fprintf(stdout,
                "Raw user collected properties\n"
                "------------------------------\n");
        for (const auto& kv : table_properties->user_collected_properties) {
          std::string prop_name = kv.first;
          std::string prop_val = Slice(kv.second).ToString(true);
          fprintf(stdout, "  # %s: 0x%s\n", prop_name.c_str(),
                  prop_val.c_str());
        }
      }
    }
  }
  if (show_summary) {
    fprintf(stdout, "total number of files: %" PRIu64 "\n", total_num_files);
    fprintf(stdout, "total number of data blocks: %" PRIu64 "\n",
            total_num_data_blocks);
    fprintf(stdout, "total data block size: %" PRIu64 "\n",
            total_data_block_size);
    fprintf(stdout, "total index block size: %" PRIu64 "\n",
            total_index_block_size);
    fprintf(stdout, "total filter block size: %" PRIu64 "\n",
            total_filter_block_size);
  }
  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
