// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/VectorBuildMaterializer.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/VectorArray.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/vector/VectorBuilderUtils.h"
#include "index/vector/VectorTypeUtils.h"

namespace milvus::indexbuilder {

bool
VectorPrimaryLayout::IsValid(size_t logical_row) const {
    AssertInfo(logical_row < logical_rows_,
               "vector primary layout row {} is out of range {}",
               logical_row,
               logical_rows_);
    return ((validity_[logical_row >> 3] >>
             static_cast<unsigned>(logical_row & 7)) &
            1U) != 0;
}

bool
VectorPrimaryLayout::Cursor::Done() const {
    AssertInfo(layout_ != nullptr, "vector primary layout cursor is unbound");
    const bool done = logical_row_ == layout_->logical_rows_;
    if (done) {
        AssertInfo(physical_row_ == layout_->physical_rows_,
                   "vector primary layout cursor consumed {} of {} physical "
                   "rows",
                   physical_row_,
                   layout_->physical_rows_);
    }
    return done;
}

VectorPrimaryLayout::Row
VectorPrimaryLayout::Cursor::Next() {
    AssertInfo(!Done(), "vector primary layout cursor is exhausted");
    const bool valid = layout_->IsValid(logical_row_++);
    if (!valid) {
        return {.valid = false, .physical_id = 0};
    }
    AssertInfo(physical_row_ < layout_->physical_rows_,
               "vector primary layout physical cursor exceeds {}",
               layout_->physical_rows_);
    return {.valid = true, .physical_id = physical_row_++};
}

void
VectorPrimaryLayout::Append(size_t logical_rows, ValidityView validity) {
    AssertInfo(
        logical_rows <= std::numeric_limits<size_t>::max() - logical_rows_,
        "vector primary logical row count overflows size_t");
    const auto next_rows = logical_rows_ + logical_rows;
    AssertInfo(next_rows <= std::numeric_limits<size_t>::max() - 7,
               "vector primary validity byte count overflows size_t");
    validity_.resize((next_rows + 7) / 8, 0);
    nullable_ = nullable_ || static_cast<bool>(validity);
    size_t added_physical = 0;
    for (size_t row = 0; row < logical_rows; ++row) {
        if (!validity || validity[static_cast<int64_t>(row)]) {
            const auto bit = logical_rows_ + row;
            validity_[bit >> 3] |=
                static_cast<uint8_t>(1U << static_cast<unsigned>(bit & 7));
            ++added_physical;
        }
    }
    AssertInfo(
        added_physical <= std::numeric_limits<size_t>::max() - physical_rows_,
        "vector primary physical row count overflows size_t");
    logical_rows_ = next_rows;
    physical_rows_ += added_physical;
}

bool
IsSupportedVectorScalarInfoType(DataType type) {
    switch (type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::STRING:
        case DataType::VARCHAR:
            return true;
        default:
            return false;
    }
}

struct VectorScalarInfoAccumulator::Impl {
    virtual ~Impl() = default;

    virtual void
    Add(const FieldDataPtr& batch) = 0;

    virtual VectorScalarInfo
    Finish() = 0;
};

namespace {

bool
CompatibleScalarType(DataType actual, DataType expected) {
    return actual == expected ||
           ((actual == DataType::STRING || actual == DataType::VARCHAR) &&
            (expected == DataType::STRING || expected == DataType::VARCHAR)) ||
           ((actual == DataType::INT64 || actual == DataType::TIMESTAMPTZ) &&
            (expected == DataType::INT64 || expected == DataType::TIMESTAMPTZ));
}

template <typename T>
class TypedScalarInfoAccumulator final
    : public VectorScalarInfoAccumulator::Impl {
 public:
    TypedScalarInfoAccumulator(FieldId field_id,
                               DataType field_type,
                               const VectorPrimaryLayout& layout)
        : field_id_(field_id),
          field_type_(field_type),
          layout_(layout),
          cursor_(layout.NewCursor()) {
    }

    void
    Add(const FieldDataPtr& batch) override {
        AssertInfo(!finished_,
                   "cannot add to a finished vector scalar-info accumulator");
        if (batch == nullptr) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar source produced a null field-data "
                      "batch");
        }
        if (!CompatibleScalarType(batch->get_data_type(), field_type_)) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar field {} produced type {}, expected {}",
                      field_id_.get(),
                      batch->get_data_type(),
                      field_type_);
        }

        auto* typed_batch = dynamic_cast<FieldData<T>*>(batch.get());
        AssertInfo(typed_batch != nullptr,
                   "optional scalar field {} has an incompatible field-data "
                   "layout",
                   field_id_.get());

        const auto rows = batch->Length();
        if (rows_seen_ > layout_.LogicalRows() ||
            rows > layout_.LogicalRows() - rows_seen_) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar field {} exceeds primary row count {}",
                      field_id_.get(),
                      layout_.LogicalRows());
        }
        const auto* values = static_cast<const T*>(typed_batch->Data());
        AssertInfo(values != nullptr || rows == 0,
                   "optional scalar field {} has null data for {} rows",
                   field_id_.get(),
                   rows);
        const bool nullable = batch->IsNullable();
        const auto* valid = nullable ? batch->ValidData() : nullptr;
        AssertInfo(!nullable || rows == 0 || valid != nullptr,
                   "nullable optional scalar field {} has no validity bitmap",
                   field_id_.get());
        for (size_t row = 0; row < rows; ++row) {
            if (nullable &&
                ((valid[row >> 3] >> static_cast<unsigned>(row & 7)) & 1U) ==
                    0) {
                ThrowInfo(DataFormatBroken,
                          "optional scalar field {} contains NULL at row {}",
                          field_id_.get(),
                          rows_seen_ + row);
            }
            const auto primary = cursor_.Next();
            if (!primary.valid) {
                continue;
            }
            if (primary.physical_id > std::numeric_limits<uint32_t>::max()) {
                ThrowInfo(Unsupported,
                          "vector scalar-info physical row {} exceeds uint32",
                          primary.physical_id);
            }
            categories_[values[row]].push_back(
                static_cast<uint32_t>(primary.physical_id));
        }
        rows_seen_ += rows;
    }

    VectorScalarInfo
    Finish() override {
        AssertInfo(!finished_,
                   "vector scalar-info accumulator was already finished");
        finished_ = true;
        if (rows_seen_ != layout_.LogicalRows()) {
            ThrowInfo(DataFormatBroken,
                      "optional scalar field {} produced {} rows, expected {}",
                      field_id_.get(),
                      rows_seen_,
                      layout_.LogicalRows());
        }
        AssertInfo(cursor_.Done(),
                   "vector scalar-info cursor did not consume its layout");
        if (categories_.size() <= 1) {
            VectorScalarInfo result;
            result.emplace(field_id_.get(),
                           std::vector<std::vector<uint32_t>>{});
            return result;
        }
        if (categories_.size() >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
            ThrowInfo(Unsupported,
                      "vector scalar-info category count exceeds uint32");
        }
        std::vector<std::vector<uint32_t>> groups;
        groups.reserve(categories_.size());
        for (auto& [_, rows] : categories_) {
            groups.push_back(std::move(rows));
        }
        VectorScalarInfo result;
        result.emplace(field_id_.get(), std::move(groups));
        return result;
    }

 private:
    FieldId field_id_;
    DataType field_type_{DataType::NONE};
    const VectorPrimaryLayout& layout_;
    VectorPrimaryLayout::Cursor cursor_;
    std::unordered_map<T, std::vector<uint32_t>> categories_;
    size_t rows_seen_{0};
    bool finished_{false};
};

template <typename T>
std::unique_ptr<VectorScalarInfoAccumulator::Impl>
MakeAccumulator(FieldId field_id,
                DataType field_type,
                const VectorPrimaryLayout& layout) {
    return std::make_unique<TypedScalarInfoAccumulator<T>>(
        field_id, field_type, layout);
}

}  // namespace

VectorScalarInfoAccumulator::VectorScalarInfoAccumulator(
    FieldId field_id, DataType field_type, const VectorPrimaryLayout& layout) {
    switch (field_type) {
        case DataType::BOOL:
            impl_ = MakeAccumulator<bool>(field_id, field_type, layout);
            break;
        case DataType::INT8:
            impl_ = MakeAccumulator<int8_t>(field_id, field_type, layout);
            break;
        case DataType::INT16:
            impl_ = MakeAccumulator<int16_t>(field_id, field_type, layout);
            break;
        case DataType::INT32:
            impl_ = MakeAccumulator<int32_t>(field_id, field_type, layout);
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            impl_ = MakeAccumulator<int64_t>(field_id, field_type, layout);
            break;
        case DataType::FLOAT:
            impl_ = MakeAccumulator<float>(field_id, field_type, layout);
            break;
        case DataType::DOUBLE:
            impl_ = MakeAccumulator<double>(field_id, field_type, layout);
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
            impl_ = MakeAccumulator<std::string>(field_id, field_type, layout);
            break;
        default:
            ThrowInfo(Unsupported,
                      "optional scalar field {} has unsupported type {}",
                      field_id.get(),
                      field_type);
    }
}

VectorScalarInfoAccumulator::~VectorScalarInfoAccumulator() = default;
VectorScalarInfoAccumulator::VectorScalarInfoAccumulator(
    VectorScalarInfoAccumulator&&) noexcept = default;
VectorScalarInfoAccumulator&
VectorScalarInfoAccumulator::operator=(VectorScalarInfoAccumulator&&) noexcept =
    default;

void
VectorScalarInfoAccumulator::Add(const FieldDataPtr& batch) {
    AssertInfo(impl_ != nullptr,
               "vector scalar-info accumulator has no implementation");
    impl_->Add(batch);
}

VectorScalarInfo
VectorScalarInfoAccumulator::Finish() && {
    AssertInfo(impl_ != nullptr,
               "vector scalar-info accumulator has no implementation");
    return impl_->Finish();
}

VectorBuildMaterializer::VectorBuildMaterializer(
    DataType field_type,
    DataType value_type,
    int64_t dim,
    int64_t expected_rows,
    const index::IndexFamily& family,
    const index::BuildParams& params)
    : field_type_(field_type),
      value_type_(value_type),
      dim_(dim),
      observed_dim_(dim),
      expected_rows_(expected_rows) {
    index::DispatchPhysicalVectorDataType(
        value_type,
        [&]<typename T>() {
            if constexpr (std::is_same_v<T, sparse_u32_f32>) {
                AssertInfo(field_type != DataType::VECTOR_ARRAY,
                           "VECTOR_ARRAY does not support sparse elements");
            }
            InitializeTyped<T>(family, params);
        },
        [&] {
            ThrowInfo(DataTypeInvalid,
                      "unsupported vector build value type {}",
                      value_type);
        });
}

VectorBuildMaterializer::~VectorBuildMaterializer() = default;
VectorBuildMaterializer::VectorBuildMaterializer(
    VectorBuildMaterializer&& other) noexcept
    : field_type_(other.field_type_),
      value_type_(other.value_type_),
      dim_(other.dim_),
      observed_dim_(other.observed_dim_),
      expected_rows_(other.expected_rows_),
      spec_(std::move(other.spec_)),
      layout_(std::move(other.layout_)),
      embedding_offsets_(std::move(other.embedding_offsets_)),
      scalar_info_(std::move(other.scalar_info_)),
      consumed_(other.consumed_),
      state_(std::move(other.state_)) {
    other.state_.emplace<0>();
}

VectorBuildMaterializer&
VectorBuildMaterializer::operator=(VectorBuildMaterializer&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    // Release the old typed state before replacing it.
    state_.emplace<0>();
    field_type_ = other.field_type_;
    value_type_ = other.value_type_;
    dim_ = other.dim_;
    observed_dim_ = other.observed_dim_;
    expected_rows_ = other.expected_rows_;
    spec_ = std::move(other.spec_);
    layout_ = std::move(other.layout_);
    embedding_offsets_ = std::move(other.embedding_offsets_);
    scalar_info_ = std::move(other.scalar_info_);
    consumed_ = other.consumed_;
    state_ = std::move(other.state_);
    other.state_.emplace<0>();
    return *this;
}

void
VectorBuildMaterializer::AssertPresent(const char* operation) const {
    AssertInfo(!std::holds_alternative<std::monostate>(state_),
               "cannot {} a moved-from vector materializer",
               operation);
}

void
VectorBuildMaterializer::AssertOpen(const char* operation) const {
    AssertPresent(operation);
    AssertInfo(
        !consumed_, "cannot {} a consumed vector materializer", operation);
}

template <typename T>
void
VectorBuildMaterializer::InitializeTyped(const index::IndexFamily& family,
                                         const index::BuildParams& params) {
    auto builder =
        index::BuilderRegistry<index::VectorBuildInput<T>>::Instance().Create(
            family, params);
    AssertInfo(builder != nullptr,
               "index family {} has no resident vector builder for type {}",
               family,
               value_type_);
    AssertInfo(
        field_type_ == value_type_ || field_type_ == DataType::VECTOR_ARRAY,
        "vector materializer field type {} disagrees with value type {}",
        field_type_,
        value_type_);
    spec_ = builder->InputSpec();
    if (field_type_ == DataType::VECTOR_ARRAY && !spec_.side_inputs.empty()) {
        ThrowInfo(Unsupported,
                  "VECTOR_ARRAY optional scalar input is not migrated");
    }
    if (field_type_ == DataType::VECTOR_ARRAY) {
        embedding_offsets_.emplace_back(0);
    }
    state_.template emplace<TypedState<T>>(
        TypedState<T>{std::move(builder), {}});
}

const index::BuilderInputSpec&
VectorBuildMaterializer::InputSpec() const {
    AssertPresent("inspect input spec on");
    return spec_;
}

void
VectorBuildMaterializer::Add(const FieldDataPtr& batch) {
    AssertOpen("add to");
    ValidateBatch(batch);
    AssertInfo(layout_.LogicalRows() <= index::vector_builder::CheckedSize(
                                            expected_rows_, "expected row") &&
                   batch->Length() <= index::vector_builder::CheckedSize(
                                          expected_rows_, "expected row") -
                                          layout_.LogicalRows(),
               "vector source exceeds expected row count {}",
               expected_rows_);
    std::visit(
        [this, &batch](auto& state) {
            using S = std::decay_t<decltype(state)>;
            if constexpr (!std::is_same_v<S, std::monostate>) {
                AppendBatch(state, batch);
            }
        },
        state_);
}

const VectorPrimaryLayout&
VectorBuildMaterializer::PrimaryLayout() const {
    AssertOpen("read layout from");
    return layout_;
}

void
VectorBuildMaterializer::SetScalarInfo(VectorScalarInfo scalar_info) {
    AssertOpen("set scalar input on");
    AssertInfo(!spec_.side_inputs.empty(),
               "vector builder did not declare scalar side input");
    AssertInfo(!scalar_info_.has_value(),
               "vector scalar side input was already delivered");
    scalar_info_.emplace(std::move(scalar_info));
}

storage::ArtifactPtr
VectorBuildMaterializer::Build() && {
    AssertOpen("build");
    consumed_ = true;
    AssertInfo(layout_.LogicalRows() == index::vector_builder::CheckedSize(
                                            expected_rows_, "expected row"),
               "vector source produced {} rows, expected {}",
               layout_.LogicalRows(),
               expected_rows_);
    AssertInfo(spec_.side_inputs.empty() || scalar_info_.has_value(),
               "vector builder requires declared scalar input delivery");

    std::vector<std::vector<index::VectorScalarCategoryGroup>> category_views;
    std::vector<index::VectorScalarFieldGroups> field_views;
    if (scalar_info_.has_value()) {
        category_views.reserve(scalar_info_->size());
        field_views.reserve(scalar_info_->size());
        for (const auto& [field_id, categories] : *scalar_info_) {
            auto& views = category_views.emplace_back();
            views.reserve(categories.size());
            for (const auto& rows : categories) {
                views.push_back({std::span<const uint32_t>(rows)});
            }
            field_views.push_back(
                {FieldId(field_id),
                 std::span<const index::VectorScalarCategoryGroup>(views)});
        }
    }
    return std::visit(
        [this, &field_views](auto& state) -> storage::ArtifactPtr {
            using S = std::decay_t<decltype(state)>;
            if constexpr (std::is_same_v<S, std::monostate>) {
                ThrowInfo(UnexpectedError,
                          "moved-from vector materializer cannot build");
                return nullptr;
            } else {
                return BuildTyped(state, field_views);
            }
        },
        state_);
}

void
VectorBuildMaterializer::ValidateBatch(const FieldDataPtr& batch) const {
    AssertInfo(batch != nullptr,
               "vector source produced a null field-data batch");
    AssertInfo(batch->get_data_type() == field_type_,
               "vector source type {} disagrees with {}",
               batch->get_data_type(),
               field_type_);
    AssertInfo(batch->Length() <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "vector batch row count exceeds int64");
    const auto valid_rows = index::vector_builder::CheckedSize(
        batch->get_valid_rows(), "valid vector row count");
    AssertInfo(valid_rows <= batch->Length(),
               "valid vector rows {} exceed logical rows {}",
               valid_rows,
               batch->Length());
    AssertInfo(!batch->IsNullable() || batch->Length() == 0 ||
                   batch->ValidData() != nullptr,
               "nullable vector batch has no validity bitmap");
    size_t counted = 0;
    auto validity = batch->IsNullable()
                        ? ValidityView::FromPacked(batch->ValidData())
                        : ValidityView{};
    for (size_t row = 0; row < batch->Length(); ++row) {
        counted += !validity || validity[static_cast<int64_t>(row)];
    }
    AssertInfo(counted == valid_rows,
               "vector validity has {} rows, field-data has {} physical rows",
               counted,
               valid_rows);
}

template <typename T>
size_t
VectorBuildMaterializer::BatchElementCount(const FieldDataBase& batch,
                                           size_t physical_rows) const {
    using ValueType = typename TypedState<T>::ValueType;
    const auto row_bytes = vector_bytes_per_element(value_type_, dim_);
    AssertInfo(row_bytes % sizeof(ValueType) == 0,
               "vector row bytes are not aligned to physical value type");
    AssertInfo(
        physical_rows == 0 ||
            row_bytes <= std::numeric_limits<size_t>::max() / physical_rows,
        "vector batch byte size overflows size_t");
    const auto expected_bytes = physical_rows * row_bytes;
    AssertInfo(batch.DataSize() >= 0 &&
                   static_cast<uint64_t>(batch.DataSize()) == expected_bytes,
               "vector batch has {} bytes, expected {}",
               batch.DataSize(),
               expected_bytes);
    return expected_bytes / sizeof(ValueType);
}

template <typename T>
void
VectorBuildMaterializer::AppendBatch(TypedState<T>& state,
                                     const FieldDataPtr& batch) {
    using ValueType = typename TypedState<T>::ValueType;
    const auto validity = batch->IsNullable()
                              ? ValidityView::FromPacked(batch->ValidData())
                              : ValidityView{};
    const auto valid_rows = index::vector_builder::CheckedSize(
        batch->get_valid_rows(), "valid vector row count");

    if (field_type_ == DataType::VECTOR_ARRAY) {
        auto* arrays = dynamic_cast<FieldData<VectorArray>*>(batch.get());
        AssertInfo(arrays != nullptr,
                   "VECTOR_ARRAY field-data has the wrong layout");
        AssertInfo(arrays->get_element_type() == value_type_,
                   "VECTOR_ARRAY element type {} disagrees with {}",
                   arrays->get_element_type(),
                   value_type_);
        AssertInfo(arrays->get_dim() == dim_,
                   "VECTOR_ARRAY dimension {} disagrees with {}",
                   arrays->get_dim(),
                   dim_);
        const auto* compact = static_cast<const VectorArray*>(batch->Data());
        AssertInfo(compact != nullptr || valid_rows == 0,
                   "VECTOR_ARRAY batch has null compact values");
        size_t total_bytes = 0;
        for (size_t row = 0; row < valid_rows; ++row) {
            const auto& value = compact[row];
            AssertInfo(value.get_element_type() == value_type_ &&
                           value.dim() == dim_ && value.physical_length() >= 0,
                       "VECTOR_ARRAY value {} has incompatible shape",
                       row);
            const auto vector_count =
                static_cast<size_t>(value.physical_length());
            const auto row_bytes = vector_bytes_per_element(value_type_, dim_);
            AssertInfo(vector_count == 0 ||
                           row_bytes <= std::numeric_limits<size_t>::max() /
                                            vector_count,
                       "VECTOR_ARRAY value {} byte size overflows size_t",
                       row);
            AssertInfo(value.byte_size() == vector_count * row_bytes,
                       "VECTOR_ARRAY value {} has {} bytes, expected {}",
                       row,
                       value.byte_size(),
                       vector_count * row_bytes);
            AssertInfo(value.byte_size() % sizeof(ValueType) == 0,
                       "VECTOR_ARRAY value {} byte size is misaligned",
                       row);
            const auto count = value.byte_size() / sizeof(ValueType);
            const auto* begin =
                reinterpret_cast<const ValueType*>(value.data());
            AssertInfo(begin != nullptr || count == 0,
                       "VECTOR_ARRAY value {} has null data",
                       row);
            if (count != 0) {
                state.values.insert(state.values.end(), begin, begin + count);
            }
            AssertInfo(vector_count <= std::numeric_limits<size_t>::max() -
                                           embedding_offsets_.back(),
                       "VECTOR_ARRAY physical row count overflows size_t");
            embedding_offsets_.push_back(embedding_offsets_.back() +
                                         vector_count);
            AssertInfo(value.byte_size() <=
                           std::numeric_limits<size_t>::max() - total_bytes,
                       "VECTOR_ARRAY byte count overflows size_t");
            total_bytes += value.byte_size();
        }
        AssertInfo(batch->DataSize() >= 0 &&
                       static_cast<uint64_t>(batch->DataSize()) == total_bytes,
                   "VECTOR_ARRAY batch has {} bytes, expected {}",
                   batch->DataSize(),
                   total_bytes);
    } else if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        auto* sparse = dynamic_cast<FieldData<SparseFloatVector>*>(batch.get());
        AssertInfo(sparse != nullptr,
                   "sparse vector field-data has the wrong layout");
        observed_dim_ = std::max(observed_dim_, sparse->Dim());
        if (state.values.empty() && layout_.LogicalRows() == 0 &&
            !batch->IsNullable()) {
            state.values.reserve(index::vector_builder::CheckedSize(
                expected_rows_, "expected row"));
        }
        const auto* begin = static_cast<const ValueType*>(batch->Data());
        AssertInfo(begin != nullptr || valid_rows == 0,
                   "sparse vector batch has null physical rows");
        if (valid_rows != 0) {
            state.values.insert(state.values.end(), begin, begin + valid_rows);
        }
    } else {
        AssertInfo(batch->get_dim() == dim_,
                   "vector batch dimension {} disagrees with {}",
                   batch->get_dim(),
                   dim_);
        const auto elements = BatchElementCount<T>(*batch, valid_rows);
        if (state.values.empty() && layout_.LogicalRows() == 0 &&
            !batch->IsNullable()) {
            const auto row_elements =
                vector_bytes_per_element(value_type_, dim_) / sizeof(ValueType);
            const auto expected = index::vector_builder::CheckedSize(
                expected_rows_, "expected row");
            AssertInfo(row_elements == 0 ||
                           expected <= std::numeric_limits<size_t>::max() /
                                           row_elements,
                       "vector tensor element count overflows size_t");
            state.values.reserve(expected * row_elements);
        }
        const auto* begin = static_cast<const ValueType*>(batch->Data());
        AssertInfo(begin != nullptr || elements == 0,
                   "vector batch has null physical data");
        if (elements != 0) {
            state.values.insert(state.values.end(), begin, begin + elements);
        }
    }
    layout_.Append(batch->Length(), validity);
}

template <typename T>
storage::ArtifactPtr
VectorBuildMaterializer::BuildTyped(
    TypedState<T>& state,
    std::span<const index::VectorScalarFieldGroups> field_views) {
    using ValueType = typename TypedState<T>::ValueType;
    const std::span<const ValueType> physical_values = state.values;
    const auto physical_rows = field_type_ == DataType::VECTOR_ARRAY
                                   ? embedding_offsets_.back()
                                   : layout_.PhysicalRows();
    AssertInfo(physical_rows <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "vector physical row count exceeds int64");
    std::optional<std::span<const size_t>> offsets;
    if (field_type_ == DataType::VECTOR_ARRAY) {
        offsets.emplace(embedding_offsets_);
    }
    index::VectorBuildInput<T> input{
        .physical_values = physical_values,
        .logical_rows = static_cast<int64_t>(layout_.LogicalRows()),
        .physical_rows = static_cast<int64_t>(physical_rows),
        .dim = observed_dim_,
        .parent_validity = layout_.Validity(),
        .embedding_offsets = offsets,
        .scalar_fields = field_views,
    };
    auto builder = std::move(state.builder);
    try {
        return std::move(*builder).Build(input);
    } catch (...) {
        // Destroy the engine-side borrower while input owners and view arrays
        // are still alive.
        builder.reset();
        throw;
    }
}

}  // namespace milvus::indexbuilder
