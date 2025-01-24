
#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;

    // Release callback
    void (*release)(struct ArrowSchema*);
    // Opaque producer-specific data
    void* private_data;
};

struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;

    // Release callback
    void (*release)(struct ArrowArray*);
    // Opaque producer-specific data
    void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    // Callback to get the stream type
    // (will be the same for all arrays in the stream).
    //
    // Return value: 0 if successful, an `errno`-compatible error code otherwise.
    //
    // If successful, the ArrowSchema must be released independently from the stream.
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

    // Callback to get the next array
    // (if no error and the array is released, the stream has ended)
    //
    // Return value: 0 if successful, an `errno`-compatible error code otherwise.
    //
    // If successful, the ArrowArray must be released independently from the stream.
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

    // Callback to get optional detailed error information.
    // This must only be called if the last stream operation failed
    // with a non-0 return code.
    //
    // Return value: pointer to a null-terminated character array describing
    // the last error, or NULL if no description is available.
    //
    // The returned pointer is only valid until the next operation on this stream
    // (including release).
    const char* (*get_last_error)(struct ArrowArrayStream*);

    // Release callback: release the stream's own resources.
    // Note that arrays returned by `get_next` must be individually released.
    void (*release)(struct ArrowArrayStream*);

    // Opaque producer-specific data
    void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE

#ifdef __cplusplus
}
#endif
