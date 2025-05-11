#include "index/NgramInvertedIndex.h"
#include "exec/expression/Expr.h"

namespace milvus::index {
constexpr const char* TMP_NGRAM_INVERTED_LOG_PREFIX =
    "/tmp/milvus/ngram-inverted-index-log/";

NgramInvertedIndex::NgramInvertedIndex(const storage::FileManagerContext& ctx,
                                       uintptr_t min_gram,
                                       uintptr_t max_gram)
    : min_gram_(min_gram), max_gram_(max_gram) {
    schema_ = ctx.fieldDataMeta.field_schema;
    field_id_ = ctx.fieldDataMeta.field_id;
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);

    if (ctx.for_loading_index) {
        path_ = disk_file_manager_->GetLocalNgramIndexPrefix();
    } else {
        auto prefix = disk_file_manager_->GetTextIndexIdentifier();
        path_ = std::string(TMP_NGRAM_INVERTED_LOG_PREFIX) + prefix;
        boost::filesystem::create_directories(path_);
        d_type_ = TantivyDataType::Keyword;
        std::string field_name =
            std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field_name.c_str(), path_.c_str(), min_gram, max_gram);
    }
}

void
NgramInvertedIndex::BuildWithFieldData(const std::vector<FieldDataPtr>& datas) {
    AssertInfo(schema_.data_type() == proto::schema::DataType::String ||
                   schema_.data_type() == proto::schema::DataType::VarChar,
               "schema data type is {}",
               schema_.data_type());
    auto build_start_time = std::chrono::system_clock::now();

    InvertedIndexTantivy<std::string>::BuildWithFieldData(datas);

    auto build_end_time = std::chrono::system_clock::now();
    auto build_duration =
        std::chrono::duration<double>(build_end_time - build_start_time)
            .count();
    LOG_INFO(
        "build ngram inverted index done for field id:{}, build duration: {}s",
        field_id_,
        build_duration);
}

void
NgramInvertedIndex::Load(milvus::tracer::TraceContext ctx,
                         const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load ngram index");

    // todo: handle null offset file

    disk_file_manager_->CacheNgramIndexToDisk(index_files.value());
    AssertInfo(
        tantivy_index_exist(path_.c_str()), "index not exist: {}", path_);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(path_.c_str(),
                                                     milvus::index::SetBitset);
}

std::optional<TargetBitmap>
NgramInvertedIndex::InnerMatchQuery(const std::string& literal,
                                    exec::SegmentExpr* segment) {
    if (literal.length() < min_gram_) {
        return std::nullopt;
    }

    TargetBitmap bitset{static_cast<size_t>(Count())};
    wrapper_->inner_match_ngram(literal, min_gram_, max_gram_, &bitset);

    // Post filtering: if the literal length is larger than the max_gram
    // we need to filter out the bitset
    if (literal.length() > max_gram_) {
        auto bitset_off = 0;
        TargetBitmapView res(bitset);
        TargetBitmap valid(res.size(), true);
        TargetBitmapView valid_res(valid.data(), valid.size());

        auto execute_sub_batch = [&bitset, &bitset_off, &literal](
                                     const std::string_view* data,
                                     const bool* valid_data,
                                     const int32_t* offsets,
                                     const int size,
                                     TargetBitmapView res,
                                     TargetBitmapView valid_res) {
            auto next_off_option = res.find_next(bitset_off);
            while (next_off_option.has_value()) {
                auto next_off = next_off_option.value();
                if (next_off >= bitset_off + size) {
                    break;
                }
                auto data_pos = next_off - bitset_off;
                if (data[data_pos].find(literal) == std::string::npos) {
                    res[next_off] = false;
                }
                next_off_option = res.find_next(next_off);
            }
            bitset_off += size;
        };

        segment->ProcessDataChunks<std::string_view>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res);
    }

    return std::optional<TargetBitmap>(std::move(bitset));
}

}  // namespace milvus::index
