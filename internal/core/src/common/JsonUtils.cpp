#include "common/JsonUtils.h"

namespace milvus {

// Parse a JSON Pointer into unescaped path segments
std::vector<std::string>
parse_json_pointer(const std::string& pointer) {
    std::vector<std::string> tokens;
    if (pointer.empty())
        return tokens;  // Root path (entire document)
    if (pointer[0] != '/') {
        throw std::invalid_argument(
            "Invalid JSON Pointer: must start with '/'");
    }
    size_t start = 1;
    while (start < pointer.size()) {
        size_t end = pointer.find('/', start);
        if (end == std::string::npos)
            end = pointer.size();
        std::string token = pointer.substr(start, end - start);
        // Replace ~1 with / and ~0 with ~
        size_t pos = 0;
        while ((pos = token.find("~1", pos)) != std::string::npos) {
            token.replace(pos, 2, "/");
            pos += 1;  // Avoid infinite loops on overlapping replacements
        }
        pos = 0;
        while ((pos = token.find("~0", pos)) != std::string::npos) {
            token.replace(pos, 2, "~");
            pos += 1;
        }
        tokens.push_back(token);
        start = end + 1;
    }
    return tokens;
}

// Check if a JSON Pointer path exists
bool
path_exists(const simdjson::dom::element& root,
            const std::vector<std::string>& tokens) {
    simdjson::dom::element current = root;
    for (const auto& token : tokens) {
        if (current.type() == simdjson::dom::element_type::OBJECT) {
            auto obj = current.get_object();
            if (obj.error())
                return false;
            auto next = obj.value().at_key(token);
            if (next.error())
                return false;
            current = next.value();
        } else if (current.type() == simdjson::dom::element_type::ARRAY) {
            if (token == "-")
                return false;  // "-" is invalid for existence checks
            char* endptr;
            long index = strtol(token.c_str(), &endptr, 10);
            if (*endptr != '\0' || index < 0)
                return false;  // Not a valid index
            auto arr = current.get_array();
            if (arr.error())
                return false;
            if (static_cast<size_t>(index) >= arr.value().size())
                return false;
            auto next = arr.value().at(index);
            if (next.error())
                return false;
            current = next.value();
        } else {
            return false;  // Path cannot be resolved
        }
    }
    return true;
}
}  // namespace milvus
