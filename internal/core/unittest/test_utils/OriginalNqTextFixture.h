// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

// The fixture JSON retains the original text's separate attribution/license,
// source revision, immutable physical row mapping and verified snapshot hash.
// These are original NQ records, not generated strings or renamed Cohere IDs.
struct OriginalNqTextRow {
    int64_t snapshot_offset;
    std::string id;
    std::string title;
    std::string text;
};

inline std::vector<OriginalNqTextRow>
LoadOriginalNqTextRows() {
    const auto path =
        std::filesystem::path(__FILE__).parent_path().parent_path() /
        "test_data/nq_original_text_1024.json";
    std::ifstream input(path);
    if (!input.good()) {
        throw std::runtime_error("missing checked-in original NQ text fixture");
    }
    const auto data = nlohmann::json::parse(input);
    std::vector<OriginalNqTextRow> rows;
    rows.reserve(data.at("rows").size());
    for (const auto& row : data.at("rows")) {
        rows.push_back({row.at("snapshot_offset").get<int64_t>(),
                        row.at("id").get<std::string>(),
                        row.at("title").get<std::string>(),
                        row.at("text").get<std::string>()});
    }
    return rows;
}
