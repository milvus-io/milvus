#include "SegmentNaive.h"
#include "SegmentSmallIndex.h"

namespace milvus::segcore {

// seems to be deprecated
struct ColumnBasedDataChunk {
    std::vector<std::vector<float>> entity_vecs;

    static ColumnBasedDataChunk
    from(const RowBasedRawData& source, const Schema& schema) {
        ColumnBasedDataChunk dest;
        auto count = source.count;
        auto raw_data = reinterpret_cast<const char*>(source.raw_data);
        auto align = source.sizeof_per_row;
        for (auto& field : schema) {
            auto len = field.get_sizeof();
            Assert(len % sizeof(float) == 0);
            std::vector<float> new_col(len * count / sizeof(float));
            for (int64_t i = 0; i < count; ++i) {
                memcpy(new_col.data() + i * len / sizeof(float), raw_data + i * align, len);
            }
            dest.entity_vecs.push_back(std::move(new_col));
            // offset the raw_data
            raw_data += len / sizeof(float);
        }
        return dest;
    }
};

int
TestABI() {
    return 42;
}

std::unique_ptr<SegmentBase>
CreateSegment(SchemaPtr schema) {
    auto segment = std::make_unique<SegmentSmallIndex>(schema);
    return segment;
}
}  // namespace milvus::segcore
