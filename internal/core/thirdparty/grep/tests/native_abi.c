#include "milvus_grep.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* Independently checked against the Rust layout test on 64-bit targets. */
#if UINTPTR_MAX == UINT64_MAX
_Static_assert(sizeof(MgGrepError) == 512, "error layout");
_Static_assert(sizeof(MgGrepBytes) == 16 && offsetof(MgGrepBytes, len) == 8, "bytes layout");
_Static_assert(sizeof(MgGrepMatchOptions) == 12 && offsetof(MgGrepMatchOptions, boundary) == 8, "options layout");
_Static_assert(sizeof(MgGrepSpecView) == 32 && offsetof(MgGrepSpecView, options) == 16, "spec layout");
_Static_assert(sizeof(MgGrepCompileLimits) == 16 && offsetof(MgGrepCompileLimits, max_regex_bytes) == 0 && offsetof(MgGrepCompileLimits, max_dfa_cache_bytes) == 8, "compile limits layout");
_Static_assert(sizeof(MgGrepSpan) == 16 && offsetof(MgGrepSpan, end) == 8, "span layout");
_Static_assert(sizeof(MgGrepLine) == 32 && offsetof(MgGrepLine, spans) == 16 && offsetof(MgGrepLine, span_count) == 24, "line layout");
_Static_assert(sizeof(MgGrepTextResult) == 16 && offsetof(MgGrepTextResult, line_count) == 8, "text layout");
_Static_assert(sizeof(MgGrepBatchView) == 16 && offsetof(MgGrepBatchView, text_count) == 8, "batch layout");
#endif

static MgGrepBytes bytes(const char *s) {
    MgGrepBytes b = {(const uint8_t *)s, strlen(s)};
    return b;
}

int main(void) {
    MgGrepCompileLimits cl = {1048576, 1048576};
    uint64_t el = {200000000};
    MgGrepBytes p = bytes("error");
    MgGrepSpecView spec = {&p, 1, {0, 0, 0}};
    MgGrepError error; /* Output storage does not need caller initialization. */
    MgGrepPlan *plan = NULL;
    MgGrepSession *session = NULL;
    MgGrepResult *result = NULL;

    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_OK);
    assert(error.message[0] == 0 && plan != NULL);
    assert(mg_grep_session_create(plan, &session, &error) == MG_GREP_OK);
    mg_grep_plan_free(plan);
    char *input = (char *)malloc(32);
    assert(input != NULL);
    strcpy(input, "error: error\r\nnext");
    MgGrepBytes batch[] = {bytes("done"), bytes(input), {NULL, 0}};
    assert(mg_grep_execute_batch(session, batch, 3, el, &result, &error) == MG_GREP_OK);
    free(input);
    mg_grep_session_free(session);
    const MgGrepBatchView *view = mg_grep_result_view(result);
    assert(view == mg_grep_result_view(result));
    assert(view->text_count == 3 && view->texts[0].lines == NULL);
    assert(view->texts[2].line_count == 0 && view->texts[2].lines == NULL);
    const MgGrepLine *line = view->texts[1].lines;
    assert(view->texts[1].line_count == 1);
    assert(line->content.len == 13 && memcmp(line->content.data, "error: error\r", 13) == 0);
    assert(line->span_count == 2 && line->spans[0].start == 0 && line->spans[0].end == 5);
    assert(line->spans[1].start == 7 && line->spans[1].end == 12);
    mg_grep_result_free(result);

    /* All handle-producing failures must clear an otherwise valid output slot. */
    plan = (MgGrepPlan *)(uintptr_t)1;
    assert(mg_grep_compile(NULL, &cl, &plan, &error) == MG_GREP_INVALID_ARGUMENT);
    assert(plan == NULL && error.message[0] != 0);
    assert(mg_grep_compile(&spec, &cl, NULL, &error) == MG_GREP_INVALID_ARGUMENT);
    session = (MgGrepSession *)(uintptr_t)1;
    assert(mg_grep_session_create(NULL, &session, &error) == MG_GREP_INVALID_ARGUMENT && session == NULL);
    spec.options.case_mode = 99;
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_INVALID_ARGUMENT && plan == NULL);
    spec.options.case_mode = 0;
    p = bytes("[");
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_INVALID_PATTERN);
    p = bytes("[a-z]+");
    cl.max_regex_bytes = 1;
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_ENGINE_BUILD_ERROR && plan == NULL);
    cl.max_regex_bytes = 1048576;
    p.data = NULL; p.len = 1;
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_INVALID_ARGUMENT);

    /* Empty-pattern matches, UTF-8 byte coordinates, and NULL empty content. */
    p.data = NULL; p.len = 0;
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_OK && error.message[0] == 0);
    assert(mg_grep_session_create(plan, &session, NULL) == MG_GREP_OK);
    mg_grep_plan_free(plan);
    MgGrepBytes unicode_batch[] = {bytes("\xc3\xa9"), bytes("\n"), {NULL, 0}};
    assert(mg_grep_execute_batch(session, unicode_batch, 3, el, &result, &error) == MG_GREP_OK);
    view = mg_grep_result_view(result);
    line = view->texts[0].lines;
    assert(line->span_count == 2 && line->spans[0].start == 0 && line->spans[1].start == 2);
    assert(view->texts[1].lines[0].content.data == NULL && view->texts[1].lines[0].content.len == 0);
    assert(view->texts[1].lines[0].span_count == 1);
    assert(view->texts[2].lines == NULL);
    mg_grep_result_free(result);
    assert(mg_grep_execute_batch(session, NULL, 0, el, &result, &error) == MG_GREP_OK);
    view = mg_grep_result_view(result);
    assert(view->text_count == 0 && view->texts == NULL);
    mg_grep_result_free(result);

    const uint8_t invalid[] = {255};
    MgGrepBytes bad = {invalid, sizeof(invalid)};
    p = bad;
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_INVALID_UTF8 && plan == NULL);
    result = (MgGrepResult *)(uintptr_t)1;
    assert(mg_grep_execute_batch(session, &bad, 1, el, &result, &error) == MG_GREP_INVALID_UTF8 && result == NULL);
    assert(mg_grep_execute_batch(session, NULL, 0, 0, &result, &error) == MG_GREP_INVALID_ARGUMENT && result == NULL);
    el = 1;
    assert(mg_grep_execute_batch(session, NULL, 0, el, &result, &error) == MG_GREP_RESOURCE_LIMIT && result == NULL);
    el = 200000000;
    MgGrepBytes good = bytes("ok");
    assert(mg_grep_execute_batch(session, &good, 1, el, &result, &error) == MG_GREP_OK);
    mg_grep_result_free(result);

    /* Use a nonmatching pattern so this exercises input size, not the
       result budget for millions of empty-pattern spans. */
    mg_grep_session_free(session);
    p = bytes("needle");
    assert(mg_grep_compile(&spec, &cl, &plan, &error) == MG_GREP_OK);
    assert(mg_grep_session_create(plan, &session, &error) == MG_GREP_OK);
    mg_grep_plan_free(plan);

    /* Large input has no separate byte cap; cancellation still applies. */
    const size_t large_size = 9 * 1024 * 1024;
    char *large = (char *)malloc(large_size);
    assert(large != NULL);
    memset(large, 'x', large_size);
    MgGrepBytes long_input = {(const uint8_t *)large, large_size};
    assert(mg_grep_execute_batch(session, &long_input, 1, el, &result, &error) == MG_GREP_OK);
    mg_grep_result_free(result);
    free(large);
    mg_grep_session_cancel(session);
    mg_grep_session_cancel(session);
    assert(mg_grep_execute_batch(session, NULL, 0, el, &result, &error) == MG_GREP_CANCELLED && result == NULL);
    mg_grep_session_free(session);

    mg_grep_plan_free(NULL);
    mg_grep_session_cancel(NULL);
    mg_grep_session_free(NULL);
    mg_grep_result_free(NULL);
    assert(mg_grep_result_view(NULL) == NULL);
    return 0;
}
