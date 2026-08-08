// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "pyudf/pyudf_runtime.h"

#include <Python.h>

#include <google/protobuf/message_lite.h>

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "pb/cgo_msg.pb.h"
#include "pyudf/pyudf.h"

namespace milvus::pyudf {
namespace {

class GilGuard {
 public:
    GilGuard() : state_(PyGILState_Ensure()) {
    }

    ~GilGuard() {
        PyGILState_Release(state_);
    }

    GilGuard(const GilGuard&) = delete;
    GilGuard&
    operator=(const GilGuard&) = delete;

 private:
    PyGILState_STATE state_;
};

std::string
PythonExceptionString() {
    if (!PyErr_Occurred()) {
        return "Python operation failed without an exception";
    }

    PyObject* exception_type = nullptr;
    PyObject* exception_value = nullptr;
    PyObject* traceback = nullptr;
    PyErr_Fetch(&exception_type, &exception_value, &traceback);
    PyErr_NormalizeException(&exception_type, &exception_value, &traceback);

    std::string message;
    PyObject* traceback_module = PyImport_ImportModule("traceback");
    if (traceback_module != nullptr) {
        PyObject* formatted = PyObject_CallMethod(
            traceback_module,
            "format_exception",
            "OOO",
            exception_type == nullptr ? Py_None : exception_type,
            exception_value == nullptr ? Py_None : exception_value,
            traceback == nullptr ? Py_None : traceback);
        if (formatted != nullptr) {
            PyObject* separator = PyUnicode_FromString("");
            if (separator != nullptr) {
                PyObject* joined = PyUnicode_Join(separator, formatted);
                if (joined != nullptr) {
                    const char* text = PyUnicode_AsUTF8(joined);
                    if (text != nullptr) {
                        message = text;
                    }
                    Py_DECREF(joined);
                }
                Py_DECREF(separator);
            }
            Py_DECREF(formatted);
        }
        Py_DECREF(traceback_module);
    }

    if (message.empty() && exception_value != nullptr) {
        PyObject* rendered = PyObject_Str(exception_value);
        if (rendered != nullptr) {
            const char* text = PyUnicode_AsUTF8(rendered);
            if (text != nullptr) {
                message = text;
            }
            Py_DECREF(rendered);
        }
    }
    if (message.empty()) {
        message = "Python operation failed";
    }

    Py_XDECREF(traceback);
    Py_XDECREF(exception_value);
    Py_XDECREF(exception_type);
    PyErr_Clear();
    return message;
}

[[noreturn]] void
ThrowPythonFailure(const char* operation) {
    auto python_error = PythonExceptionString();
    ThrowInfo(
        ErrorCode::UnexpectedError, "py_udf: {}: {}", operation, python_error);
}

[[noreturn]] void
ThrowFunctionFailure(const char* operation) {
    auto python_error = PythonExceptionString();
    throw PyUDFFunctionError(
        fmt::format("py_udf: {}: {}", operation, python_error));
}

bool
IsBlank(const std::string& value) {
    return value.empty() ||
           std::all_of(value.begin(), value.end(), [](unsigned char c) {
               return std::isspace(c) != 0;
           });
}

constexpr int kMaxParamNestingDepth = 64;

bool
IsValidUTF8(const std::string& value) {
    const auto* text = reinterpret_cast<const unsigned char*>(value.data());
    size_t position = 0;
    while (position < value.size()) {
        const unsigned char first = text[position++];
        if (first <= 0x7f) {
            continue;
        }

        size_t continuation_count = 0;
        uint32_t codepoint = 0;
        if ((first & 0xe0) == 0xc0) {
            continuation_count = 1;
            codepoint = first & 0x1f;
        } else if ((first & 0xf0) == 0xe0) {
            continuation_count = 2;
            codepoint = first & 0x0f;
        } else if ((first & 0xf8) == 0xf0) {
            continuation_count = 3;
            codepoint = first & 0x07;
        } else {
            return false;
        }
        if (position + continuation_count > value.size()) {
            return false;
        }
        for (size_t index = 0; index < continuation_count; ++index) {
            const unsigned char next = text[position++];
            if ((next & 0xc0) != 0x80) {
                return false;
            }
            codepoint = (codepoint << 6) | (next & 0x3f);
        }
        if ((continuation_count == 1 && codepoint < 0x80) ||
            (continuation_count == 2 && codepoint < 0x800) ||
            (continuation_count == 3 && codepoint < 0x10000) ||
            codepoint > 0x10ffff ||
            (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
            return false;
        }
    }
    return true;
}

class OwnedPyObject {
 public:
    OwnedPyObject() noexcept = default;

    static OwnedPyObject
    Adopt(PyObject* new_reference) noexcept {
        return OwnedPyObject(new_reference);
    }

    static OwnedPyObject
    FromBorrowed(PyObject* borrowed_reference) noexcept {
        return OwnedPyObject(Py_XNewRef(borrowed_reference));
    }

    ~OwnedPyObject() {
        Py_XDECREF(object_);
    }

    OwnedPyObject(const OwnedPyObject&) = delete;
    OwnedPyObject&
    operator=(const OwnedPyObject&) = delete;

    OwnedPyObject(OwnedPyObject&& other) noexcept : object_(other.release()) {
    }

    OwnedPyObject&
    operator=(OwnedPyObject&& other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    [[nodiscard]] PyObject*
    get() const noexcept {
        return object_;
    }

    explicit
    operator bool() const noexcept {
        return object_ != nullptr;
    }

    [[nodiscard]] PyObject*
    release() noexcept {
        return std::exchange(object_, nullptr);
    }

    void
    reset(PyObject* new_reference = nullptr) noexcept {
        auto* old_reference = std::exchange(object_, new_reference);
        Py_XDECREF(old_reference);
    }

 private:
    explicit OwnedPyObject(PyObject* object) noexcept : object_(object) {
    }

    PyObject* object_ = nullptr;
};

class RuntimeState {
 public:
    void
    Initialize() {
        std::call_once(initialization_once_, [this]() {
            try {
                InitializeOnce();
                std::lock_guard lock(mutex_);
                initialized_ = true;
            } catch (...) {
                std::lock_guard lock(mutex_);
                initialization_error_ = std::current_exception();
            }
        });

        std::exception_ptr initialization_error;
        {
            std::lock_guard lock(mutex_);
            if (initialized_) {
                return;
            }
            initialization_error = initialization_error_;
        }
        if (initialization_error != nullptr) {
            std::rethrow_exception(initialization_error);
        }
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: CPython initialization did not run");
    }

    bool
    initialized() const {
        std::lock_guard lock(mutex_);
        return initialized_;
    }

    PyObject*
    load_instances() const {
        return load_instances_;
    }

    PyObject*
    close_instances() const {
        return close_instances_;
    }

    PyObject*
    import_array() const {
        return import_array_;
    }

    PyObject*
    make_chunked_array() const {
        return make_chunked_array_;
    }

    PyObject*
    export_array() const {
        return export_array_;
    }

    PyObject*
    freeze_params() const {
        return freeze_params_;
    }

    PyObject*
    run_transform_query() const {
        return run_transform_query_;
    }

 private:
    void
    InitializeOnce() {
        if (Py_IsInitialized()) {
            ThrowInfo(
                ErrorCode::UnexpectedError,
                "py_udf: CPython was initialized before the isolated PyUDF "
                "runtime");
        }
        PyConfig config;
        PyConfig_InitIsolatedConfig(&config);
        config.use_environment = 0;
        config.user_site_directory = 0;
        config.site_import = 1;
        config.parse_argv = 0;

        const auto initialize_status = Py_InitializeFromConfig(&config);
        PyConfig_Clear(&config);
        if (PyStatus_Exception(initialize_status)) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "py_udf: cannot initialize isolated CPython: {}",
                      initialize_status.err_msg == nullptr
                          ? "unknown error"
                          : initialize_status.err_msg);
        }

        // Py_InitializeFromConfig returns with the initial GIL held. CPython
        // cannot be safely reinitialized after wrapper import begins, so an
        // import/contract failure is permanent for this process.
        try {
            ImportTrustedWrapper();
        } catch (...) {
            PyEval_SaveThread();
            throw;
        }
        PyEval_SaveThread();
    }

    void
    ImportTrustedWrapper() {
        // The selected Python environment must already contain the trusted
        // milvus_pyudf_runtime package in its system site-packages. Isolated
        // mode ignores PYTHONPATH and user site while site initialization makes
        // that preinstalled package available through the normal import path.

        // CPython owns its process lifetime after a successful initialization.
        // There is intentionally no Py_FinalizeEx path in this runtime.
        auto module =
            OwnedPyObject::Adopt(PyImport_ImportModule("milvus_pyudf_runtime"));
        if (module.get() == nullptr) {
            ThrowPythonFailure("cannot import trusted runtime wrapper");
        }
        auto api_version = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "RUNTIME_API_VERSION"));
        if (api_version.get() == nullptr) {
            ThrowPythonFailure("trusted runtime wrapper has no API version");
        }
        if (!PyLong_Check(api_version.get()) ||
            PyLong_AsLong(api_version.get()) != 1) {
            ThrowInfo(
                ErrorCode::UnexpectedError,
                "py_udf: trusted runtime wrapper has incompatible API version");
        }
        auto loader = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "load_instances"));
        if (loader.get() == nullptr || !PyCallable_Check(loader.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable load_instances");
        }
        auto closer = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "close_instances"));
        if (closer.get() == nullptr || !PyCallable_Check(closer.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable close_instances");
        }
        auto import_array = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "import_array"));
        if (import_array.get() == nullptr ||
            !PyCallable_Check(import_array.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable import_array");
        }
        auto make_chunked_array = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "make_chunked_array"));
        if (make_chunked_array.get() == nullptr ||
            !PyCallable_Check(make_chunked_array.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable make_chunked_array");
        }
        auto export_array = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "export_array"));
        if (export_array.get() == nullptr ||
            !PyCallable_Check(export_array.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable export_array");
        }
        auto freeze_params = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "freeze_params"));
        if (freeze_params.get() == nullptr ||
            !PyCallable_Check(freeze_params.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable freeze_params");
        }
        auto run_transform_query = OwnedPyObject::Adopt(
            PyObject_GetAttrString(module.get(), "run_transform_query"));
        if (run_transform_query.get() == nullptr ||
            !PyCallable_Check(run_transform_query.get())) {
            ThrowPythonFailure(
                "trusted runtime wrapper has no callable run_transform_query");
        }

        module_ = module.release();
        load_instances_ = loader.release();
        close_instances_ = closer.release();
        import_array_ = import_array.release();
        make_chunked_array_ = make_chunked_array.release();
        export_array_ = export_array.release();
        freeze_params_ = freeze_params.release();
        run_transform_query_ = run_transform_query.release();
    }

    mutable std::mutex mutex_;
    std::once_flag initialization_once_;
    bool initialized_ = false;
    std::exception_ptr initialization_error_;
    PyObject* module_ = nullptr;
    PyObject* load_instances_ = nullptr;
    PyObject* close_instances_ = nullptr;
    PyObject* import_array_ = nullptr;
    PyObject* make_chunked_array_ = nullptr;
    PyObject* export_array_ = nullptr;
    PyObject* freeze_params_ = nullptr;
    PyObject* run_transform_query_ = nullptr;
};

RuntimeState&
Runtime() {
    // Intentionally leaked: Python objects must not be decref'ed during static
    // destruction after the interpreter's process-lifetime shutdown sequence.
    static auto* state = new RuntimeState();
    return *state;
}

OwnedPyObject
ParamValueToPython(const milvus::proto::schema::FunctionParamValue& value,
                   int depth);

OwnedPyObject
ParamObjectToPython(const milvus::proto::schema::FunctionParamObject& object,
                    int depth) {
    if (depth > kMaxParamNestingDepth) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: run params exceed maximum nesting depth");
    }
    auto result = OwnedPyObject::Adopt(PyDict_New());
    if (result.get() == nullptr) {
        ThrowPythonFailure("cannot allocate Python params object");
    }
    for (const auto& [key, value] : object.fields()) {
        auto python_value = ParamValueToPython(value, depth + 1);
        if (PyDict_SetItemString(
                result.get(), key.c_str(), python_value.get()) != 0) {
            ThrowPythonFailure("cannot populate Python params object");
        }
    }
    return result;
}

OwnedPyObject
ParamValueToPython(const milvus::proto::schema::FunctionParamValue& value,
                   int depth) {
    if (depth > kMaxParamNestingDepth) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: run params exceed maximum nesting depth");
    }
    using ParamValue = milvus::proto::schema::FunctionParamValue;
    switch (value.value_case()) {
        case ParamValue::kBoolValue:
            return OwnedPyObject::FromBorrowed(value.bool_value() ? Py_True
                                                                  : Py_False);
        case ParamValue::kInt64Value:
            return OwnedPyObject::Adopt(
                PyLong_FromLongLong(value.int64_value()));
        case ParamValue::kDoubleValue:
            return OwnedPyObject::Adopt(
                PyFloat_FromDouble(value.double_value()));
        case ParamValue::kStringValue:
            return OwnedPyObject::Adopt(PyUnicode_FromStringAndSize(
                value.string_value().data(), value.string_value().size()));
        case ParamValue::kBytesValue:
            return OwnedPyObject::Adopt(PyBytes_FromStringAndSize(
                value.bytes_value().data(), value.bytes_value().size()));
        case ParamValue::kArrayValue: {
            auto result = OwnedPyObject::Adopt(
                PyTuple_New(value.array_value().values_size()));
            if (result.get() == nullptr) {
                ThrowPythonFailure("cannot allocate Python params array");
            }
            for (int index = 0; index < value.array_value().values_size();
                 ++index) {
                auto item = ParamValueToPython(
                    value.array_value().values(index), depth + 1);
                PyTuple_SET_ITEM(result.get(), index, item.release());
            }
            return result;
        }
        case ParamValue::kObjectValue:
            return ParamObjectToPython(value.object_value(), depth + 1);
        case ParamValue::VALUE_NOT_SET:
            ThrowInfo(ErrorCode::UnexpectedError,
                      "py_udf: run params contain an unset value");
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "py_udf: run params contain an unknown value type");
}

milvus::proto::cgo::PyUDFRunParams
ParseRunParams(const uint8_t* serialized_params,
               uint64_t serialized_params_len) {
    if (serialized_params_len == 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params are empty");
    }
    if (serialized_params == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params pointer is nil");
    }
    if (serialized_params_len > static_cast<uint64_t>(INT_MAX)) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params exceed protobuf parse limit");
    }
    milvus::proto::cgo::PyUDFRunParams params;
    if (!params.ParseFromArray(serialized_params,
                               static_cast<int>(serialized_params_len))) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params are malformed");
    }
    if (params.ByteSizeLong() == 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params have no protocol fields");
    }
    if (IsBlank(params.resource_name()) ||
        !IsValidUTF8(params.resource_name()) || IsBlank(params.stage()) ||
        !IsValidUTF8(params.stage())) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params have blank or invalid UTF-8 "
                  "protocol fields");
    }
    if (!params.has_udf_params()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized run params have no udf_params");
    }
    return params;
}

class CPythonPyUDFResource final : public PyUDFResource {
 public:
    CPythonPyUDFResource(PyObject* instances,
                         std::string resource_name,
                         std::string stage)
        : instances_(instances),
          resource_name_(std::move(resource_name)),
          stage_(std::move(stage)) {
    }

    std::unique_ptr<PyUDFResult>
    Run(PyUDFInvocation& invocation,
        const uint8_t* serialized_params,
        uint64_t serialized_params_len) override {
        std::lock_guard lock(mutex_);
        if (closed_) {
            ThrowInfo(ErrorCode::UnexpectedError, "py_udf: resource is closed");
        }
        auto run_params =
            ParseRunParams(serialized_params, serialized_params_len);
        if (run_params.resource_name() != resource_name_ ||
            run_params.stage() != stage_) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "py_udf: run params resource or stage does not match "
                      "loaded resource");
        }
        invocation.ValidatePopulated();
        for (int32_t input = 0; input < invocation.num_inputs(); ++input) {
            for (int32_t chunk = 0; chunk < invocation.num_chunks(); ++chunk) {
                if (invocation.input_array(input, chunk)->length !=
                    invocation.chunk_size(chunk)) {
                    ThrowInfo(ErrorCode::UnexpectedError,
                              "py_udf: invocation input {} chunk {} has {} "
                              "rows, expected {}",
                              input,
                              chunk,
                              invocation.input_array(input, chunk)->length,
                              invocation.chunk_size(chunk));
                }
            }
        }

        GilGuard gil;
        std::vector<std::vector<OwnedPyObject>> input_chunks;
        input_chunks.reserve(static_cast<size_t>(invocation.num_inputs()));
        std::vector<OwnedPyObject> chunked_inputs;
        chunked_inputs.reserve(static_cast<size_t>(invocation.num_inputs()));
        for (int32_t input = 0; input < invocation.num_inputs(); ++input) {
            std::vector<OwnedPyObject> chunks;
            chunks.reserve(static_cast<size_t>(invocation.num_chunks()));
            auto chunk_list = OwnedPyObject::Adopt(
                PyList_New(static_cast<Py_ssize_t>(invocation.num_chunks())));
            if (chunk_list.get() == nullptr) {
                ThrowPythonFailure("cannot allocate PyArrow input chunk list");
            }
            for (int32_t chunk = 0; chunk < invocation.num_chunks(); ++chunk) {
                auto array_address = OwnedPyObject::Adopt(
                    PyLong_FromVoidPtr(invocation.input_array(input, chunk)));
                auto schema_address = OwnedPyObject::Adopt(
                    PyLong_FromVoidPtr(invocation.input_schema(input, chunk)));
                if (array_address.get() == nullptr ||
                    schema_address.get() == nullptr) {
                    ThrowPythonFailure(
                        "cannot build PyArrow input descriptor addresses");
                }
                auto array = OwnedPyObject::Adopt(
                    PyObject_CallFunctionObjArgs(Runtime().import_array(),
                                                 array_address.get(),
                                                 schema_address.get(),
                                                 nullptr));
                if (array.get() == nullptr) {
                    ThrowPythonFailure("cannot import PyArrow input array");
                }
                if (invocation.input_array(input, chunk)->release != nullptr ||
                    invocation.input_schema(input, chunk)->release != nullptr) {
                    ThrowInfo(ErrorCode::UnexpectedError,
                              "py_udf: PyArrow import did not consume input {} "
                              "chunk {} descriptors",
                              input,
                              chunk);
                }
                if (PyList_SetItem(chunk_list.get(),
                                   static_cast<Py_ssize_t>(chunk),
                                   Py_NewRef(array.get())) != 0) {
                    ThrowPythonFailure(
                        "cannot append PyArrow input chunk to column");
                }
                chunks.push_back(std::move(array));
            }
            if (invocation.num_chunks() > 0) {
                auto chunked = OwnedPyObject::Adopt(PyObject_CallOneArg(
                    Runtime().make_chunked_array(), chunk_list.get()));
                if (chunked.get() == nullptr) {
                    ThrowPythonFailure(
                        "cannot assemble PyArrow input ChunkedArray");
                }
                chunked_inputs.push_back(std::move(chunked));
            }
            input_chunks.push_back(std::move(chunks));
        }

        auto raw_params = ParamObjectToPython(run_params.udf_params(), 0);
        auto python_params = OwnedPyObject::Adopt(
            PyObject_CallOneArg(Runtime().freeze_params(), raw_params.get()));
        if (python_params.get() == nullptr) {
            ThrowPythonFailure("cannot freeze Python UDF params");
        }
        auto loaded_instance =
            OwnedPyObject::Adopt(PySequence_GetItem(instances_, 0));
        if (loaded_instance.get() == nullptr) {
            ThrowPythonFailure("cannot select loaded PyUDF instance");
        }

        std::vector<std::vector<OwnedPyObject>> output_chunks_by_output;
        std::vector<OwnedPyObject> output_types;
        for (int32_t chunk = 0; chunk < invocation.num_chunks(); ++chunk) {
            auto columns = OwnedPyObject::Adopt(
                PyTuple_New(static_cast<Py_ssize_t>(invocation.num_inputs())));
            if (columns.get() == nullptr) {
                ThrowPythonFailure("cannot allocate transform_query columns");
            }
            for (int32_t input = 0; input < invocation.num_inputs(); ++input) {
                PyTuple_SET_ITEM(columns.get(),
                                 input,
                                 Py_NewRef(input_chunks[input][chunk].get()));
            }
            auto expected_rows = OwnedPyObject::Adopt(
                PyLong_FromLongLong(invocation.chunk_size(chunk)));
            if (expected_rows.get() == nullptr) {
                ThrowPythonFailure("cannot build transform_query row count");
            }
            auto outputs = OwnedPyObject::Adopt(
                PyObject_CallFunctionObjArgs(Runtime().run_transform_query(),
                                             loaded_instance.get(),
                                             python_params.get(),
                                             columns.get(),
                                             expected_rows.get(),
                                             nullptr));
            if (outputs.get() == nullptr) {
                ThrowFunctionFailure("Python UDF transform_query failed");
            }
            const auto output_count = PySequence_Size(outputs.get());
            if (output_count < 0) {
                ThrowPythonFailure("cannot read transform_query outputs");
            }
            if (chunk == 0) {
                output_chunks_by_output.resize(
                    static_cast<size_t>(output_count));
                output_types.resize(static_cast<size_t>(output_count));
                for (auto& chunks : output_chunks_by_output) {
                    chunks.reserve(
                        static_cast<size_t>(invocation.num_chunks()));
                }
            } else if (static_cast<size_t>(output_count) !=
                       output_chunks_by_output.size()) {
                throw PyUDFFunctionError(
                    fmt::format("py_udf: transform_query output count changed "
                                "from {} to {} "
                                "at chunk {}",
                                output_chunks_by_output.size(),
                                output_count,
                                chunk));
            }
            for (Py_ssize_t output = 0; output < output_count; ++output) {
                auto array = OwnedPyObject::Adopt(
                    PySequence_GetItem(outputs.get(), output));
                if (array.get() == nullptr) {
                    ThrowPythonFailure("cannot read transform_query output");
                }
                auto type = OwnedPyObject::Adopt(
                    PyObject_GetAttrString(array.get(), "type"));
                if (type.get() == nullptr) {
                    ThrowPythonFailure(
                        "transform_query output has no Arrow type");
                }
                if (chunk == 0) {
                    output_types[static_cast<size_t>(output)] = std::move(type);
                } else {
                    const auto equal = PyObject_RichCompareBool(
                        output_types[static_cast<size_t>(output)].get(),
                        type.get(),
                        Py_EQ);
                    if (equal < 0) {
                        ThrowPythonFailure(
                            "cannot compare transform_query output types");
                    }
                    if (equal == 0) {
                        throw PyUDFFunctionError(fmt::format(
                            "py_udf: transform_query output {} type changed at "
                            "chunk {}",
                            output,
                            chunk));
                    }
                }
                output_chunks_by_output[static_cast<size_t>(output)].push_back(
                    std::move(array));
            }
        }

        std::vector<int32_t> output_chunks(output_chunks_by_output.size(),
                                           invocation.num_chunks());
        auto result = std::make_unique<PyUDFResult>(
            static_cast<int32_t>(output_chunks.size()), output_chunks.data());
        for (size_t output = 0; output < output_chunks_by_output.size();
             ++output) {
            for (int32_t chunk = 0; chunk < invocation.num_chunks(); ++chunk) {
                auto array_address = OwnedPyObject::Adopt(PyLong_FromVoidPtr(
                    result->output_array(static_cast<int32_t>(output), chunk)));
                auto schema_address = OwnedPyObject::Adopt(
                    PyLong_FromVoidPtr(result->output_schema(
                        static_cast<int32_t>(output), chunk)));
                if (array_address.get() == nullptr ||
                    schema_address.get() == nullptr) {
                    ThrowPythonFailure(
                        "cannot build PyArrow output descriptor addresses");
                }
                auto exported =
                    OwnedPyObject::Adopt(PyObject_CallFunctionObjArgs(
                        Runtime().export_array(),
                        output_chunks_by_output[output][chunk].get(),
                        array_address.get(),
                        schema_address.get(),
                        nullptr));
                if (exported.get() == nullptr) {
                    ThrowPythonFailure("cannot export PyArrow output array");
                }
            }
        }
        return result;
    }

    void
    Close() override {
        std::lock_guard lock(mutex_);
        if (closed_) {
            return;
        }
        closed_ = true;

        GilGuard gil;
        std::exception_ptr first_failure;
        if (instances_ != nullptr) {
            auto result = OwnedPyObject::Adopt(PyObject_CallFunctionObjArgs(
                Runtime().close_instances(), instances_, nullptr));
            if (result.get() == nullptr) {
                try {
                    ThrowFunctionFailure("Python resource close failed");
                } catch (...) {
                    first_failure = std::current_exception();
                }
            }
            Py_DECREF(instances_);
            instances_ = nullptr;
        }
        if (first_failure != nullptr) {
            std::rethrow_exception(first_failure);
        }
    }

    ~CPythonPyUDFResource() override {
        // DeletePyUDFResource calls Close. This fallback protects C++ ownership
        // paths and cannot report close failures from a destructor.
        try {
            Close();
        } catch (...) {
        }
    }

 private:
    std::mutex mutex_;
    bool closed_ = false;
    PyObject* instances_ = nullptr;
    std::string resource_name_;
    std::string stage_;
};

void
ValidateRequest(const uint8_t* serialized_request,
                uint64_t serialized_request_len,
                milvus::proto::cgo::PyUDFLoadRequest* request) {
    if (serialized_request_len == 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized load request is empty");
    }
    if (serialized_request == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized load request pointer is nil");
    }
    if (serialized_request_len > static_cast<uint64_t>(INT_MAX)) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized load request exceeds protobuf parse "
                  "limit");
    }
    if (!request->ParseFromArray(serialized_request,
                                 static_cast<int>(serialized_request_len))) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized load request is malformed");
    }
    if (request->ByteSizeLong() == 0) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized load request has no protocol fields");
    }

    const auto valid_text = [](const std::string& value) {
        return !IsBlank(value) && IsValidUTF8(value);
    };
    if (!valid_text(request->resource_name()) ||
        !valid_text(request->resource_path()) ||
        !valid_text(request->local_path()) || !valid_text(request->stage())) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: serialized load request has blank or invalid UTF-8 "
                  "protocol fields");
    }
    if (request->instance_count() <= 0) {
        ThrowInfo(
            ErrorCode::UnexpectedError,
            "py_udf: serialized load request instance_count must be positive");
    }

    const std::filesystem::path local_path(request->local_path());
    std::error_code error;
    if (local_path.extension() != ".whl" ||
        !std::filesystem::is_regular_file(local_path, error) || error) {
        ThrowInfo(ErrorCode::FileOpenFailed,
                  "py_udf: local wheel is not a readable regular .whl file");
    }
    // Opening the exact path distinguishes an inaccessible file from a merely
    // stat-able path. The wrapper independently validates zip integrity.
    std::ifstream wheel(local_path, std::ios::binary);
    if (!wheel.good()) {
        ThrowInfo(ErrorCode::FileOpenFailed,
                  "py_udf: local wheel cannot be opened for reading");
    }
}

}  // namespace

bool
RuntimeBuildEnabled() noexcept {
    return true;
}

void
InitializeRuntime() {
    Runtime().Initialize();
}

std::unique_ptr<PyUDFResource>
LoadResource(const uint8_t* serialized_request,
             uint64_t serialized_request_len) {
    if (!Runtime().initialized()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "py_udf: runtime has not been initialized");
    }

    milvus::proto::cgo::PyUDFLoadRequest request;
    ValidateRequest(serialized_request, serialized_request_len, &request);

    GilGuard gil;
    auto resource_identity =
        OwnedPyObject::Adopt(PyLong_FromLongLong(request.resource_id()));
    if (resource_identity.get() == nullptr) {
        ThrowPythonFailure("cannot build resource identity");
    }
    auto resource_name = OwnedPyObject::Adopt(
        PyUnicode_FromString(request.resource_name().c_str()));
    auto local_path = OwnedPyObject::Adopt(
        PyUnicode_FromString(request.local_path().c_str()));
    auto stage =
        OwnedPyObject::Adopt(PyUnicode_FromString(request.stage().c_str()));
    auto instance_count =
        OwnedPyObject::Adopt(PyLong_FromLong(request.instance_count()));
    if (resource_name.get() == nullptr || local_path.get() == nullptr ||
        stage.get() == nullptr || instance_count.get() == nullptr) {
        ThrowPythonFailure("cannot build Python load arguments");
    }
    auto instances = OwnedPyObject::Adopt(
        PyObject_CallFunctionObjArgs(Runtime().load_instances(),
                                     resource_name.get(),
                                     local_path.get(),
                                     stage.get(),
                                     instance_count.get(),
                                     resource_identity.get(),
                                     nullptr));
    if (instances.get() == nullptr) {
        ThrowFunctionFailure("Python UDF load failed");
    }
    if (!PySequence_Check(instances.get()) ||
        PySequence_Size(instances.get()) != request.instance_count()) {
        ThrowInfo(
            ErrorCode::UnexpectedError,
            "py_udf: trusted wrapper returned an invalid instance sequence");
    }

    return std::make_unique<CPythonPyUDFResource>(
        instances.release(), request.resource_name(), request.stage());
}

}  // namespace milvus::pyudf
