#pragma once
#include <string>
#include <vector>
#include "simdjson/dom.h"

namespace milvus {

// Parse a JSON Pointer into unescaped path segments
std::vector<std::string>
parse_json_pointer(const std::string& pointer);

// Check if a JSON Pointer path exists
bool
path_exists(const simdjson::dom::element& root,
            const std::vector<std::string>& tokens);
}  // namespace milvus