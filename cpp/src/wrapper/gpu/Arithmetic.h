/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>
#include <cstddef>


namespace zilliz {
namespace milvus {
namespace engine {

using Bool = int8_t;
using Byte = uint8_t;
using Word = unsigned long;
using EnumType = uint64_t;

using Float32 = float;
using Float64 = double;

constexpr bool kBoolMax = std::numeric_limits<bool>::max();
constexpr bool kBoolMin = std::numeric_limits<bool>::lowest();

constexpr int8_t kInt8Max = std::numeric_limits<int8_t>::max();
constexpr int8_t kInt8Min = std::numeric_limits<int8_t>::lowest();

constexpr int16_t kInt16Max = std::numeric_limits<int16_t>::max();
constexpr int16_t kInt16Min = std::numeric_limits<int16_t>::lowest();

constexpr int32_t kInt32Max = std::numeric_limits<int32_t>::max();
constexpr int32_t kInt32Min = std::numeric_limits<int32_t>::lowest();

constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();
constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::lowest();

constexpr float kFloatMax = std::numeric_limits<float>::max();
constexpr float kFloatMin = std::numeric_limits<float>::lowest();

constexpr double kDoubleMax = std::numeric_limits<double>::max();
constexpr double kDoubleMin = std::numeric_limits<double>::lowest();

constexpr uint32_t kFloat32DecimalPrecision = std::numeric_limits<Float32>::digits10;
constexpr uint32_t kFloat64DecimalPrecision = std::numeric_limits<Float64>::digits10;


constexpr uint8_t kByteWidth = 8;
constexpr uint8_t kCharWidth = kByteWidth;
constexpr uint8_t kWordWidth = sizeof(Word) * kByteWidth;
constexpr uint8_t kEnumTypeWidth = sizeof(EnumType) * kByteWidth;

template<typename T>
inline size_t
WidthOf() { return sizeof(T) << 3; }

template<typename T>
inline size_t
WidthOf(const T &) { return sizeof(T) << 3; }


}
} // namespace lib
} // namespace zilliz
