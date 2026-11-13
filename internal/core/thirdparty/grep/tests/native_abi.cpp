#include "milvus_grep.h"
#include "tantivy-binding.h"
#include <cassert>
#include <memory>
#include <string>

int main() {
    const uint8_t pattern[] = {'a'};
    MgGrepBytes p{pattern, 1};
    MgGrepSpecView spec{&p, 1, {MG_GREP_PATTERN_LITERAL, MG_GREP_CASE_SENSITIVE, MG_GREP_BOUNDARY_NONE, 0, 0, 0, 0}};
    MgGrepCompileLimits limits{1048576, 1048576};
    MgGrepPlan* raw = nullptr;
    assert(mg_grep_compile(&spec, &limits, &raw, nullptr) == MG_GREP_OK);
    std::unique_ptr<MgGrepPlan, decltype(&mg_grep_plan_free)> plan(raw, mg_grep_plan_free);
    MgGrepSession* session_raw = nullptr;
    assert(mg_grep_session_create(plan.get(), &session_raw, nullptr) == MG_GREP_OK);
    std::unique_ptr<MgGrepSession, decltype(&mg_grep_session_free)> session(session_raw, mg_grep_session_free);
    plan.reset();
    uint64_t execution{4096};
    MgGrepResult* result_raw = nullptr;
    assert(mg_grep_execute_batch(session.get(), &p, 1, execution, &result_raw, nullptr) == MG_GREP_OK);
    std::unique_ptr<MgGrepResult, decltype(&mg_grep_result_free)> result(result_raw, mg_grep_result_free);
    session.reset();
    assert(mg_grep_result_view(result.get())->texts[0].lines[0].spans[0].end == 1);

    const std::string binary_pattern = R"(a\x00b)";
    MgGrepBytes binary{reinterpret_cast<const uint8_t*>(binary_pattern.data()), binary_pattern.size()};
    MgGrepSpecView binary_spec{&binary, 1, {MG_GREP_PATTERN_REGEX, MG_GREP_CASE_SENSITIVE, MG_GREP_BOUNDARY_NONE, 0, 0, 0, 0}};
    assert(mg_grep_compile(&binary_spec, &limits, &raw, nullptr) == MG_GREP_OK);
    plan.reset(raw);
    MgGrepBytes view;
    assert(mg_grep_plan_condition(plan.get(), &view, nullptr) == MG_GREP_OK);
    const std::string owned(reinterpret_cast<const char*>(view.data), view.len);
    plan.reset();
    assert(owned.find(std::string("a\0b", 3)) != std::string::npos);
}
