
#pragma once

#include <assert.h>
#include <string.h>

#include "arrow/c/abi.h"

#ifdef __cplusplus
extern "C" {
#endif

/// Query whether the C schema is released
static inline int
ArrowSchemaIsReleased(const struct ArrowSchema* schema) {
    return schema->release == NULL;
}

/// Mark the C schema released (for use in release callbacks)
static inline void
ArrowSchemaMarkReleased(struct ArrowSchema* schema) {
    schema->release = NULL;
}

/// Move the C schema from `src` to `dest`
///
/// Note `dest` must *not* point to a valid schema already, otherwise there
/// will be a memory leak.
static inline void
ArrowSchemaMove(struct ArrowSchema* src, struct ArrowSchema* dest) {
    assert(dest != src);
    assert(!ArrowSchemaIsReleased(src));
    memcpy(dest, src, sizeof(struct ArrowSchema));
    ArrowSchemaMarkReleased(src);
}

/// Release the C schema, if necessary, by calling its release callback
static inline void
ArrowSchemaRelease(struct ArrowSchema* schema) {
    if (!ArrowSchemaIsReleased(schema)) {
        schema->release(schema);
        assert(ArrowSchemaIsReleased(schema));
    }
}

/// Query whether the C array is released
static inline int
ArrowArrayIsReleased(const struct ArrowArray* array) {
    return array->release == NULL;
}

/// Mark the C array released (for use in release callbacks)
static inline void
ArrowArrayMarkReleased(struct ArrowArray* array) {
    array->release = NULL;
}

/// Move the C array from `src` to `dest`
///
/// Note `dest` must *not* point to a valid array already, otherwise there
/// will be a memory leak.
static inline void
ArrowArrayMove(struct ArrowArray* src, struct ArrowArray* dest) {
    assert(dest != src);
    assert(!ArrowArrayIsReleased(src));
    memcpy(dest, src, sizeof(struct ArrowArray));
    ArrowArrayMarkReleased(src);
}

/// Release the C array, if necessary, by calling its release callback
static inline void
ArrowArrayRelease(struct ArrowArray* array) {
    if (!ArrowArrayIsReleased(array)) {
        array->release(array);
        assert(ArrowArrayIsReleased(array));
    }
}

/// Query whether the C array stream is released
static inline int
ArrowArrayStreamIsReleased(const struct ArrowArrayStream* stream) {
    return stream->release == NULL;
}

/// Mark the C array stream released (for use in release callbacks)
static inline void
ArrowArrayStreamMarkReleased(struct ArrowArrayStream* stream) {
    stream->release = NULL;
}

/// Move the C array stream from `src` to `dest`
///
/// Note `dest` must *not* point to a valid stream already, otherwise there
/// will be a memory leak.
static inline void
ArrowArrayStreamMove(struct ArrowArrayStream* src,
                     struct ArrowArrayStream* dest) {
    assert(dest != src);
    assert(!ArrowArrayStreamIsReleased(src));
    memcpy(dest, src, sizeof(struct ArrowArrayStream));
    ArrowArrayStreamMarkReleased(src);
}

/// Release the C array stream, if necessary, by calling its release callback
static inline void
ArrowArrayStreamRelease(struct ArrowArrayStream* stream) {
    if (!ArrowArrayStreamIsReleased(stream)) {
        stream->release(stream);
        assert(ArrowArrayStreamIsReleased(stream));
    }
}

#ifdef __cplusplus
}
#endif
