// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <filesystem>
#include <fstream>
#include <vector>
#include <nlohmann/json.hpp>

// Original VectorDBBench Cohere train IDs, not a generated arithmetic fixture.
// Source URL, full parquet hash and extracted little-endian ID hash are stored
// alongside the unchanged values. This numeric fixture makes core regressions
// runnable without the optional large local experiment files.
inline std::vector<int64_t>
LoadOriginalCohereIds() {
    const auto path = std::filesystem::path(__FILE__).parent_path().parent_path() /
                      "test_data/cohere_original_ids_1024.json";
    std::ifstream input(path);
    if (!input.good()) {
        throw std::runtime_error("missing checked-in original Cohere ID fixture");
    }
    auto data = nlohmann::json::parse(input);
    return data.at("ids").get<std::vector<int64_t>>();
}
