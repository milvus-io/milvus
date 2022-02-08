Faiss C API
===========

Faiss provides a pure C interface, which can subsequently be used either in pure C programs or to produce bindings for programming languages with Foreign Function Interface (FFI) support. Although this is not required for the Python interface, some other programming languages (e.g. Rust and Julia) do not have SWIG support.

Compilation instructions
------------------------

The full contents of the pure C API are in the ["c_api"](c_api/) folder.
Please be sure to follow the instructions on [building the main C++ library](../INSTALL.md#step-1-compiling-the-c-faiss) first.
Then, enter the [c_api](c_api/) directory and run

  `make`

This builds the dynamic library "faiss_c", containing the full implementation of Faiss and the necessary wrappers for the C interface. It does not depend on libfaiss.a or the C++ standard library. It will also build an example program `bin/example_c`.

Using the API
-------------

The C API is composed of:

- A set of C header files comprising the main Faiss interfaces, converted for use in C. Each file follows the format `«name»_c.h`, where `«name»` is the respective name from the C++ API. For example, the file [Index_c.h](./Index_c.h) file corresponds to the base `Index` API. Functions are declared with the `faiss_` prefix (e.g. `faiss_IndexFlat_new`), whereas new types have the `Faiss` prefix (e.g. `FaissIndex`, `FaissMetricType`, ...).
- A dynamic library, compiled from the sources in the same folder, encloses the implementation of the library and wrapper functions.

The index factory is available via the `faiss_index_factory` function in `AutoTune_c.h`:

```c
FaissIndex* index = NULL;
int c = faiss_index_factory(&index, 64, "Flat", METRIC_L2);
if (c) {
    // operation failed
}
```

Most operations that you would find as member functions are available with the format `faiss_«classname»_«member»`.

```c
idx_t ntotal = faiss_Index_ntotal(index);
```

Since this is C, the index needs to be freed manually in the end:

```c
faiss_Index_free(index);
```

Error handling is done by examining the error code returned by operations with recoverable errors.
The code identifies the type of exception that rose from the implementation. Fetching the 
corresponding error message can be done by calling the function `faiss_get_last_error()` from
`error_c.h`. Getter functions and `free` functions do not return an error code.

```c
int c = faiss_Index_add(index, nb, xb);
if (c) {
    printf("%s", faiss_get_last_error());
    exit(-1);
}
```

An example is included, which is built automatically for the target `all`. It can also be built separately:

  `make bin/example_c`

Building with GPU support
-------------------------

For GPU support, a separate dynamic library in the "c_api/gpu" directory needs to be built.

  `make`

The "gpufaiss_c" dynamic library contains the GPU and CPU implementations of Faiss, which means that
it can be used in place of "faiss_c". The same library will dynamically link with the CUDA runtime
and cuBLAS.

Using the GPU with the C API
----------------------------

A standard GPU resurces object can be obtained by the name `FaissStandardGpuResources`:

```c
FaissStandardGpuResources* gpu_res = NULL;
int c = faiss_StandardGpuResources_new(&gpu_res);
if (c) {
    printf("%s", faiss_get_last_error());
    exit(-1);
}
```

Similarly to the C++ API, a CPU index can be converted to a GPU index:

```c
FaissIndex* cpu_index = NULL;
int c = faiss_index_factory(&cpu_index, d, "Flat", METRIC_L2);
if (c) { /* ... */ }
FaissGpuIndex* gpu_index = NULL;
c = faiss_index_cpu_to_gpu(gpu_res, 0, cpu_index, &gpu_index);
if (c) { /* ... */ }
```

A more complete example is available by the name `bin/example_gpu_c`.
