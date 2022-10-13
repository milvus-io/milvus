/**
 * \mainpage
 *
 * This project is a C++ API for
 * [Wasmtime](https://github.com/bytecodealliance/wasmtime). Support for the
 * C++ API is exclusively built on the [C API of
 * Wasmtime](https://docs.wasmtime.dev/c-api/), so the C++ support for this is
 * simply a single header file. To use this header file, though, it must be
 * combined with the header and binary of Wasmtime's C API. Note, though, that
 * while this header is built on top of the `wasmtime.h` header file you should
 * only need to use the contents of this header file to interact with Wasmtime.
 *
 * Examples can be [found
 * online](https://github.com/bytecodealliance/wasmtime-cpp/tree/main/examples)
 * and otherwise be sure to check out the
 * [README](https://github.com/bytecodealliance/wasmtime-cpp/blob/main/README.md)
 * for simple usage instructions. Otherwise you can dive right in to the
 * reference documentation of \ref wasmtime.hh
 *
 * \example hello.cc
 * \example gcd.cc
 * \example linking.cc
 * \example memory.cc
 * \example interrupt.cc
 * \example externref.cc
 */

/**
 * \file wasmtime.hh
 */

#ifndef WASMTIME_HH
#define WASMTIME_HH

#include <any>
#include <array>
#include <cstdio>
#include <initializer_list>
#include <iosfwd>
#include <limits>
#include <memory>
#include <optional>
#include <variant>
#include <vector>
#ifdef __cpp_lib_span
#include <span>
#endif

#include <wasmtime.h>
namespace wasmtime {

#ifdef __cpp_lib_span

    /// \brief Alias to C++20 std::span when it is available
template <typename T, std::size_t Extent = std::dynamic_extent>
using Span = std::span<T, Extent>;

#else

/// \brief Means number of elements determined at runtime
    inline constexpr size_t dynamic_extent =
            std::numeric_limits<std::size_t>::max();

/**
 * \brief Span class used when c++20 is not available
 * @tparam T Type of data
 * @tparam Extent Static size of data refered by Span class
 */
    template <typename T, std::size_t Extent = dynamic_extent> class Span {
    public:
        /// \brief Type used to iterate over this span (a raw pointer)
        using iterator = T *;

        /// \brief Constructor of Span class
        Span(T *t, std::size_t n) : ptr_{t}, size_{n} {}

        /// \brief Constructor of Span class for containers
        template <template <typename, typename> class Container>
        Span(Container<T, std::allocator<T>> &range)
                : ptr_{range.data()}, size_{range.size()} {}

        /// \brief Returns item by index
        T &operator[](ptrdiff_t idx) const {
            return ptr_[idx]; // NOLINT
        }

        /// \brief Returns pointer to data
        T *data() const { return ptr_; }

        /// \brief Returns number of data that referred by Span class
        std::size_t size() const { return size_; }

        /// \brief Returns begin iterator
        iterator begin() const { return ptr_; }

        /// \brief Returns end iterator
        iterator end() const {
            return ptr_ + size_; // NOLINT
        }

        /// \brief Returns size in bytes
        std::size_t size_bytes() const { return sizeof(T) * size_; }

    private:
        T *ptr_;
        std::size_t size_;
    };

#endif

/**
 * \brief Errors coming from Wasmtime
 *
 * This class represents an error that came from Wasmtime and contains a textual
 * description of the error that occurred.
 */
    class Error {
        std::string msg;

    public:
        /// \brief Creates an error from the raw C API representation
        ///
        /// Takes ownership of the provided `error`.
        Error(wasmtime_error_t *error) {
            wasm_byte_vec_t msg_bytes;
            wasmtime_error_message(error, &msg_bytes);
            msg = std::string(msg_bytes.data, msg_bytes.size);
            wasm_byte_vec_delete(&msg_bytes);
            wasmtime_error_delete(error);
        }

        /// \brief Creates a custom error from a custom error string.
        Error(std::string_view msg) : msg(msg) {}

        /// \brief Returns the error message associated with this error.
        const std::string &message() const { return msg; }
    };

/// \brief Used to print an error.
    inline std::ostream &operator<<(std::ostream &os, const Error &e) {
        os << e.message();
        return os;
    }

/**
 * \brief Fallible result type used for Wasmtime.
 *
 * This type is used as the return value of many methods in the Wasmtime API.
 * This behaves similarly to Rust's `Result<T, E>` and will be replaced with a
 * C++ standard when it exists.
 */
    template <typename T, typename E = Error> class Result {
        std::variant<T, E> data;

    public:
        /// \brief Creates a `Result` from its successful value.
        Result(T t) : data(std::move(t)) {}
        /// \brief Creates a `Result` from an error value.
        Result(E e) : data(std::move(e)) {}

        /// \brief Returns `true` if this result is a success, `false` if it's an
        /// error
        explicit operator bool() const { return data.index() == 0; }

        /// \brief Returns the error, if present, aborts if this is not an error.
        E &&err() { return std::get<E>(std::move(data)); }
        /// \brief Returns the error, if present, aborts if this is not an error.
        const E &&err() const { return std::get<E>(std::move(data)); }

        /// \brief Returns the success, if present, aborts if this is an error.
        T &&ok() { return std::get<T>(std::move(data)); }
        /// \brief Returns the success, if present, aborts if this is an error.
        const T &&ok() const { return std::get<T>(std::move(data)); }

        /// \brief Returns the success, if present, aborts if this is an error.
        T unwrap() {
            if (*this) {
                return this->ok();
            }
            unwrap_failed();
        }

    private:
        [[noreturn]] void unwrap_failed() {
            fprintf(stderr, "error: %s\n", this->err().message().c_str()); // NOLINT
            std::abort();
        }
    };

/// \brief Strategies passed to `Config::strategy`
    enum class Strategy {
        /// Automatically selects the compilation strategy
        Auto = WASMTIME_STRATEGY_AUTO,
        /// Requires Cranelift to be used for compilation
        Cranelift = WASMTIME_STRATEGY_CRANELIFT,
    };

/// \brief Values passed to `Config::cranelift_opt_level`
    enum class OptLevel {
        /// No extra optimizations performed
        None = WASMTIME_OPT_LEVEL_NONE,
        /// Optimize for speed
        Speed = WASMTIME_OPT_LEVEL_SPEED,
        /// Optimize for speed and generated code size
        SpeedAndSize = WASMTIME_OPT_LEVEL_SPEED_AND_SIZE,
    };

/// \brief Values passed to `Config::profiler`
    enum class ProfilingStrategy {
        /// No profiling enabled
        None = WASMTIME_PROFILING_STRATEGY_NONE,
        /// Profiling hooks via perf's jitdump
        Jitdump = WASMTIME_PROFILING_STRATEGY_JITDUMP,
        /// Profiling hooks via VTune
        Vtune = WASMTIME_PROFILING_STRATEGY_VTUNE,
    };

/**
 * \brief Configuration for Wasmtime.
 *
 * This class is used to configure Wasmtime's compilation and various other
 * settings such as enabled WebAssembly proposals.
 *
 * For more information be sure to consult the [rust
 * documentation](https://docs.wasmtime.dev/api/wasmtime/struct.Config.html).
 */
    class Config {
        friend class Engine;

        struct deleter {
            void operator()(wasm_config_t *p) const { wasm_config_delete(p); }
        };

        std::unique_ptr<wasm_config_t, deleter> ptr;

    public:
        /// \brief Creates configuration with all the default settings.
        Config() : ptr(wasm_config_new()) {}

        /// \brief Configures whether dwarf debuginfo is emitted for assisting
        /// in-process debugging.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.debug_info
        void debug_info(bool enable) {
            wasmtime_config_debug_info_set(ptr.get(), enable);
        }

        /// \brief Configures whether epochs are enabled which can be used to
        /// interrupt currently executing WebAssembly.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.epoch_interruption
        void epoch_interruption(bool enable) {
            wasmtime_config_epoch_interruption_set(ptr.get(), enable);
        }

        /// \brief Configures whether WebAssembly code will consume fuel and trap when
        /// it runs out.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.consume_fuel
        void consume_fuel(bool enable) {
            wasmtime_config_consume_fuel_set(ptr.get(), enable);
        }

        /// \brief Configures the maximum amount of native stack wasm can consume.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.max_wasm_stack
        void max_wasm_stack(size_t stack) {
            wasmtime_config_max_wasm_stack_set(ptr.get(), stack);
        }

        /// \brief Configures whether the WebAssembly threads proposal is enabled
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.wasm_threads
        void wasm_threads(bool enable) {
            wasmtime_config_wasm_threads_set(ptr.get(), enable);
        }

        /// \brief Configures whether the WebAssembly reference types proposal is
        /// enabled
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.wasm_reference_types
        void wasm_reference_types(bool enable) {
            wasmtime_config_wasm_reference_types_set(ptr.get(), enable);
        }

        /// \brief Configures whether the WebAssembly simd proposal is enabled
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.wasm_simd
        void wasm_simd(bool enable) {
            wasmtime_config_wasm_simd_set(ptr.get(), enable);
        }

        /// \brief Configures whether the WebAssembly bulk memory proposal is enabled
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.wasm_bulk_memory
        void wasm_bulk_memory(bool enable) {
            wasmtime_config_wasm_bulk_memory_set(ptr.get(), enable);
        }

        /// \brief Configures whether the WebAssembly multi value proposal is enabled
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.wasm_multi_value
        void wasm_multi_value(bool enable) {
            wasmtime_config_wasm_multi_value_set(ptr.get(), enable);
        }

        /// \brief Configures compilation strategy for wasm code.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.strategy
        void strategy(Strategy strategy) {
            wasmtime_config_strategy_set(ptr.get(),
                                         static_cast<wasmtime_strategy_t>(strategy));
        }

        /// \brief Configures whether cranelift's debug verifier is enabled
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.cranelift_debug_verifier
        void cranelift_debug_verifier(bool enable) {
            wasmtime_config_cranelift_debug_verifier_set(ptr.get(), enable);
        }

        /// \brief Configures cranelift's optimization level
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.cranelift_opt_level
        void cranelift_opt_level(OptLevel level) {
            wasmtime_config_cranelift_opt_level_set(
                    ptr.get(), static_cast<wasmtime_opt_level_t>(level));
        }

        /// \brief Configures an active wasm profiler
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.profiler
        void profiler(ProfilingStrategy profiler) {
            wasmtime_config_profiler_set(
                    ptr.get(), static_cast<wasmtime_profiling_strategy_t>(profiler));
        }

        /// \brief Configures the maximum size of memory to use a "static memory"
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.static_memory_maximum_size
        void static_memory_maximum_size(size_t size) {
            wasmtime_config_static_memory_maximum_size_set(ptr.get(), size);
        }

        /// \brief Configures the size of static memory's guard region
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.static_memory_guard_size
        void static_memory_guard_size(size_t size) {
            wasmtime_config_static_memory_guard_size_set(ptr.get(), size);
        }

        /// \brief Configures the size of dynamic memory's guard region
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.dynamic_memory_guard_size
        void dynamic_memory_guard_size(size_t size) {
            wasmtime_config_dynamic_memory_guard_size_set(ptr.get(), size);
        }

        /// \brief Loads the default cache configuration present on the system.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.cache_config_load_default
        [[nodiscard]] Result<std::monostate> cache_load_default() {
            auto *error = wasmtime_config_cache_config_load(ptr.get(), nullptr);
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /// \brief Loads cache configuration from the specified filename.
        ///
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#method.cache_config_load
        [[nodiscard]] Result<std::monostate> cache_load(const std::string &path) {
            auto *error = wasmtime_config_cache_config_load(ptr.get(), path.c_str());
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }
    };

/**
 * \brief Global compilation state in Wasmtime.
 *
 * Created with either default configuration or with a specified instance of
 * configuration, an `Engine` is used as an umbrella "session" for all other
 * operations in Wasmtime.
 */
    class Engine {
        friend class Store;
        friend class Module;
        friend class Linker;

        struct deleter {
            void operator()(wasm_engine_t *p) const { wasm_engine_delete(p); }
        };

        std::unique_ptr<wasm_engine_t, deleter> ptr;

    public:
        /// \brief Creates an engine with default compilation settings.
        Engine() : ptr(wasm_engine_new()) {}
        /// \brief Creates an engine with the specified compilation settings.
        explicit Engine(Config config)
                : ptr(wasm_engine_new_with_config(config.ptr.release())) {}

        /// \brief Increments the current epoch which may result in interrupting
        /// currently executing WebAssembly in connected stores if the epoch is now
        /// beyond the configured threshold.
        void increment_epoch() const { wasmtime_engine_increment_epoch(ptr.get()); }
    };

/**
 * \brief Converts the WebAssembly text format into the WebAssembly binary
 * format.
 *
 * This will parse the text format and attempt to translate it to the binary
 * format. Note that the text parser assumes that all WebAssembly features are
 * enabled and will parse syntax of future proposals. The exact syntax here
 * parsed may be tweaked over time.
 *
 * Returns either an error if parsing failed or the wasm binary.
 */
    [[nodiscard]] inline Result<std::vector<uint8_t>>
    wat2wasm(std::string_view wat) {
        wasm_byte_vec_t ret;
        auto *error = wasmtime_wat2wasm(wat.data(), wat.size(), &ret);
        if (error != nullptr) {
            return Error(error);
        }
        std::vector<uint8_t> vec;
        // NOLINTNEXTLINE TODO can this be done without triggering lints?
        Span<uint8_t> raw(reinterpret_cast<uint8_t *>(ret.data), ret.size);
        vec.assign(raw.begin(), raw.end());
        wasm_byte_vec_delete(&ret);
        return vec;
    }

/// Different kinds of types accepted by Wasmtime.
    enum class ValKind {
        /// WebAssembly's `i32` type
        I32,
        /// WebAssembly's `i64` type
        I64,
        /// WebAssembly's `f32` type
        F32,
        /// WebAssembly's `f64` type
        F64,
        /// WebAssembly's `v128` type from the simd proposal
        V128,
        /// WebAssembly's `externref` type from the reference types
        ExternRef,
        /// WebAssembly's `funcref` type from the reference types
        FuncRef,
    };

/// \brief Used to print a ValKind.
    inline std::ostream &operator<<(std::ostream &os, const ValKind &e) {
        switch (e) {
            case ValKind::I32:
                os << "i32";
                break;
            case ValKind::I64:
                os << "i64";
                break;
            case ValKind::F32:
                os << "f32";
                break;
            case ValKind::F64:
                os << "f64";
                break;
            case ValKind::ExternRef:
                os << "externref";
                break;
            case ValKind::FuncRef:
                os << "funcref";
                break;
            case ValKind::V128:
                os << "v128";
                break;
            default:
                abort();
        }
        return os;
    }

/**
 * \brief Type information about a WebAssembly value.
 *
 * Currently mostly just contains the `ValKind`.
 */
    class ValType {
        friend class TableType;
        friend class GlobalType;
        friend class FuncType;

        struct deleter {
            void operator()(wasm_valtype_t *p) const { wasm_valtype_delete(p); }
        };

        std::unique_ptr<wasm_valtype_t, deleter> ptr;

        static wasm_valkind_t kind_to_c(ValKind kind) {
            switch (kind) {
                case ValKind::I32:
                    return WASM_I32;
                case ValKind::I64:
                    return WASM_I64;
                case ValKind::F32:
                    return WASM_F32;
                case ValKind::F64:
                    return WASM_F64;
                case ValKind::ExternRef:
                    return WASM_ANYREF;
                case ValKind::FuncRef:
                    return WASM_FUNCREF;
                case ValKind::V128:
                    return WASMTIME_V128;
                default:
                    abort();
            }
        }

    public:
        /// \brief Non-owning reference to a `ValType`, must not be used after the
        /// original `ValType` is deleted.
        class Ref {
            friend class ValType;

            const wasm_valtype_t *ptr;

        public:
            /// \brief Instantiates from the raw C API representation.
            Ref(const wasm_valtype_t *ptr) : ptr(ptr) {}
            /// Copy constructor
            Ref(const ValType &ty) : Ref(ty.ptr.get()) {}

            /// \brief Returns the corresponding "kind" for this type.
            ValKind kind() const {
                switch (wasm_valtype_kind(ptr)) {
                    case WASM_I32:
                        return ValKind::I32;
                    case WASM_I64:
                        return ValKind::I64;
                    case WASM_F32:
                        return ValKind::F32;
                    case WASM_F64:
                        return ValKind::F64;
                    case WASM_ANYREF:
                        return ValKind::ExternRef;
                    case WASM_FUNCREF:
                        return ValKind::FuncRef;
                    case WASMTIME_V128:
                        return ValKind::V128;
                }
                std::abort();
            }
        };

        /// \brief Non-owning reference to a list of `ValType` instances. Must not be
        /// used after the original owner is deleted.
        class ListRef {
            const wasm_valtype_vec_t *list;

        public:
            /// Creates a list from the raw underlying C API.
            ListRef(const wasm_valtype_vec_t *list) : list(list) {}

            /// This list iterates over a list of `ValType::Ref` instances.
            typedef const Ref *iterator;

            /// Pointer to the beginning of iteration
            iterator begin() const {
                return reinterpret_cast<Ref *>(&list->data[0]); // NOLINT
            }

            /// Pointer to the end of iteration
            iterator end() const {
                return reinterpret_cast<Ref *>(&list->data[list->size]); // NOLINT
            }

            /// Returns how many types are in this list.
            size_t size() const { return list->size; }
        };

    private:
        Ref ref;
        ValType(wasm_valtype_t *ptr) : ptr(ptr), ref(ptr) {}

    public:
        /// Creates a new type from its kind.
        ValType(ValKind kind) : ValType(wasm_valtype_new(kind_to_c(kind))) {}
        /// Copies a `Ref` to a new owned value.
        ValType(Ref other) : ValType(wasm_valtype_copy(other.ptr)) {}
        /// Copies one type to a new one.
        ValType(const ValType &other) : ValType(wasm_valtype_copy(other.ptr.get())) {}
        /// Copies the contents of another type into this one.
        ValType &operator=(const ValType &other) {
            ptr.reset(wasm_valtype_copy(other.ptr.get()));
            return *this;
        }
        ~ValType() = default;
        /// Moves the memory owned by another value type into this one.
        ValType(ValType &&other) = default;
        /// Moves the memory owned by another value type into this one.
        ValType &operator=(ValType &&other) = default;

        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator->() { return &ref; }
        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator*() { return &ref; }
    };

/**
 * \brief Type information about a WebAssembly linear memory
 */
    class MemoryType {
        friend class Memory;

        struct deleter {
            void operator()(wasm_memorytype_t *p) const { wasm_memorytype_delete(p); }
        };

        std::unique_ptr<wasm_memorytype_t, deleter> ptr;

    public:
        /// \brief Non-owning reference to a `MemoryType`, must not be used after the
        /// original owner has been deleted.
        class Ref {
            friend class MemoryType;

            const wasm_memorytype_t *ptr;

        public:
            /// Creates a refernece from the raw C API representation.
            Ref(const wasm_memorytype_t *ptr) : ptr(ptr) {}
            /// Creates a reference from an original `MemoryType`.
            Ref(const MemoryType &ty) : Ref(ty.ptr.get()) {}

            /// Returns the minimum size, in WebAssembly pages, of this memory.
            uint64_t min() const { return wasmtime_memorytype_minimum(ptr); }

            /// Returns the maximum size, in WebAssembly pages, of this memory, if
            /// specified.
            std::optional<uint64_t> max() const {
                uint64_t max = 0;
                auto present = wasmtime_memorytype_maximum(ptr, &max);
                if (present) {
                    return max;
                }
                return std::nullopt;
            }

            /// Returns whether or not this is a 64-bit memory type.
            bool is_64() const { return wasmtime_memorytype_is64(ptr); }
        };

    private:
        Ref ref;
        MemoryType(wasm_memorytype_t *ptr) : ptr(ptr), ref(ptr) {}

    public:
        /// Creates a new 32-bit wasm memory type with the specified minimum number of
        /// pages for the minimum size. The created type will have no maximum memory
        /// size.
        explicit MemoryType(uint32_t min)
                : MemoryType(wasmtime_memorytype_new(min, false, 0, false)) {}
        /// Creates a new 32-bit wasm memory type with the specified minimum number of
        /// pages for the minimum size, and maximum number of pages for the max size.
        MemoryType(uint32_t min, uint32_t max)
                : MemoryType(wasmtime_memorytype_new(min, true, max, false)) {}

        /// Same as the `MemoryType` constructor, except creates a 64-bit memory.
        static MemoryType New64(uint64_t min) {
            return MemoryType(wasmtime_memorytype_new(min, false, 0, true));
        }

        /// Same as the `MemoryType` constructor, except creates a 64-bit memory.
        static MemoryType New64(uint64_t min, uint64_t max) {
            return MemoryType(wasmtime_memorytype_new(min, true, max, true));
        }

        /// Creates a new wasm memory type from the specified ref, making a fresh
        /// owned value.
        MemoryType(Ref other) : MemoryType(wasm_memorytype_copy(other.ptr)) {}
        /// Copies the provided type into a new type.
        MemoryType(const MemoryType &other)
                : MemoryType(wasm_memorytype_copy(other.ptr.get())) {}
        /// Copies the provided type into a new type.
        MemoryType &operator=(const MemoryType &other) {
            ptr.reset(wasm_memorytype_copy(other.ptr.get()));
            return *this;
        }
        ~MemoryType() = default;
        /// Moves the type information from another type into this one.
        MemoryType(MemoryType &&other) = default;
        /// Moves the type information from another type into this one.
        MemoryType &operator=(MemoryType &&other) = default;

        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator->() { return &ref; }
        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator*() { return &ref; }
    };

/**
 * \brief Type information about a WebAssembly table.
 */
    class TableType {
        friend class Table;

        struct deleter {
            void operator()(wasm_tabletype_t *p) const { wasm_tabletype_delete(p); }
        };

        std::unique_ptr<wasm_tabletype_t, deleter> ptr;

    public:
        /// Non-owning reference to a `TableType`, must not be used after the original
        /// owner is deleted.
        class Ref {
            friend class TableType;

            const wasm_tabletype_t *ptr;

        public:
            /// Creates a reference from the raw underlying C API representation.
            Ref(const wasm_tabletype_t *ptr) : ptr(ptr) {}
            /// Creates a reference to the provided `TableType`.
            Ref(const TableType &ty) : Ref(ty.ptr.get()) {}

            /// Returns the minimum size of this table type, in elements.
            uint32_t min() const { return wasm_tabletype_limits(ptr)->min; }

            /// Returns the maximum size of this table type, in elements, if present.
            std::optional<uint32_t> max() const {
                const auto *limits = wasm_tabletype_limits(ptr);
                if (limits->max == wasm_limits_max_default) {
                    return std::nullopt;
                }
                return limits->max;
            }

            /// Returns the type of value that is stored in this table.
            ValType::Ref element() const { return wasm_tabletype_element(ptr); }
        };

    private:
        Ref ref;
        TableType(wasm_tabletype_t *ptr) : ptr(ptr), ref(ptr) {}

    public:
        /// Creates a new table type from the specified value type and minimum size.
        /// The returned table will have no maximum size.
        TableType(ValType ty, uint32_t min) : ptr(nullptr), ref(nullptr) {
            wasm_limits_t limits;
            limits.min = min;
            limits.max = wasm_limits_max_default;
            ptr.reset(wasm_tabletype_new(ty.ptr.release(), &limits));
            ref = ptr.get();
        }
        /// Creates a new table type from the specified value type, minimum size, and
        /// maximum size.
        TableType(ValType ty, uint32_t min, uint32_t max) // NOLINT
                : ptr(nullptr), ref(nullptr) {
            wasm_limits_t limits;
            limits.min = min;
            limits.max = max;
            ptr.reset(wasm_tabletype_new(ty.ptr.release(), &limits));
            ref = ptr.get();
        }
        /// Clones the given reference into a new table type.
        TableType(Ref other) : TableType(wasm_tabletype_copy(other.ptr)) {}
        /// Copies another table type into this one.
        TableType(const TableType &other)
                : TableType(wasm_tabletype_copy(other.ptr.get())) {}
        /// Copies another table type into this one.
        TableType &operator=(const TableType &other) {
            ptr.reset(wasm_tabletype_copy(other.ptr.get()));
            return *this;
        }
        ~TableType() = default;
        /// Moves the table type resources from another type to this one.
        TableType(TableType &&other) = default;
        /// Moves the table type resources from another type to this one.
        TableType &operator=(TableType &&other) = default;

        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator->() { return &ref; }
        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator*() { return &ref; }
    };

/**
 * \brief Type information about a WebAssembly global
 */
    class GlobalType {
        friend class Global;

        struct deleter {
            void operator()(wasm_globaltype_t *p) const { wasm_globaltype_delete(p); }
        };

        std::unique_ptr<wasm_globaltype_t, deleter> ptr;

    public:
        /// Non-owning reference to a `Global`, must not be used after the original
        /// owner is deleted.
        class Ref {
            friend class GlobalType;
            const wasm_globaltype_t *ptr;

        public:
            /// Creates a new reference from the raw underlying C API representation.
            Ref(const wasm_globaltype_t *ptr) : ptr(ptr) {}
            /// Creates a new reference to the specified type.
            Ref(const GlobalType &ty) : Ref(ty.ptr.get()) {}

            /// Returns whether or not this global type is mutable.
            bool is_mutable() const {
                return wasm_globaltype_mutability(ptr) == WASM_VAR;
            }

            /// Returns the type of value stored within this global type.
            ValType::Ref content() const { return wasm_globaltype_content(ptr); }
        };

    private:
        Ref ref;
        GlobalType(wasm_globaltype_t *ptr) : ptr(ptr), ref(ptr) {}

    public:
        /// Creates a new global type from the specified value type and mutability.
        GlobalType(ValType ty, bool mut)
                : GlobalType(wasm_globaltype_new(
                ty.ptr.release(),
                (wasm_mutability_t)(mut ? WASM_VAR : WASM_CONST))) {}
        /// Clones a reference into a uniquely owned global type.
        GlobalType(Ref other) : GlobalType(wasm_globaltype_copy(other.ptr)) {}
        /// Copies othe type information into this one.
        GlobalType(const GlobalType &other)
                : GlobalType(wasm_globaltype_copy(other.ptr.get())) {}
        /// Copies othe type information into this one.
        GlobalType &operator=(const GlobalType &other) {
            ptr.reset(wasm_globaltype_copy(other.ptr.get()));
            return *this;
        }
        ~GlobalType() = default;
        /// Moves the underlying type information from another global into this one.
        GlobalType(GlobalType &&other) = default;
        /// Moves the underlying type information from another global into this one.
        GlobalType &operator=(GlobalType &&other) = default;

        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator->() { return &ref; }
        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator*() { return &ref; }
    };

/**
 * \brief Type information for a WebAssembly function.
 */
    class FuncType {
        friend class Func;
        friend class Linker;

        struct deleter {
            void operator()(wasm_functype_t *p) const { wasm_functype_delete(p); }
        };

        std::unique_ptr<wasm_functype_t, deleter> ptr;

    public:
        /// Non-owning reference to a `FuncType`, must not be used after the original
        /// owner has been deleted.
        class Ref {
            friend class FuncType;
            const wasm_functype_t *ptr;

        public:
            /// Creates a new reference from the underlying C API representation.
            Ref(const wasm_functype_t *ptr) : ptr(ptr) {}
            /// Creates a new reference to the given type.
            Ref(const FuncType &ty) : Ref(ty.ptr.get()) {}

            /// Returns the list of types this function type takes as parameters.
            ValType::ListRef params() const { return wasm_functype_params(ptr); }

            /// Returns the list of types this function type returns.
            ValType::ListRef results() const { return wasm_functype_results(ptr); }
        };

    private:
        Ref ref;
        FuncType(wasm_functype_t *ptr) : ptr(ptr), ref(ptr) {}

    public:
        /// Creates a new function type from the given list of parameters and results.
        FuncType(std::initializer_list<ValType> params,
                 std::initializer_list<ValType> results)
                : ref(nullptr) {
            *this = FuncType::from_iters(params, results);
        }

        /// Copies a reference into a uniquely owned function type.
        FuncType(Ref other) : FuncType(wasm_functype_copy(other.ptr)) {}
        /// Copies another type's information into this one.
        FuncType(const FuncType &other)
                : FuncType(wasm_functype_copy(other.ptr.get())) {}
        /// Copies another type's information into this one.
        FuncType &operator=(const FuncType &other) {
            ptr.reset(wasm_functype_copy(other.ptr.get()));
            return *this;
        }
        ~FuncType() = default;
        /// Moves type information from anothe type into this one.
        FuncType(FuncType &&other) = default;
        /// Moves type information from anothe type into this one.
        FuncType &operator=(FuncType &&other) = default;

        /// Creates a new function type from the given list of parameters and results.
        template <typename P, typename R>
        static FuncType from_iters(P params, R results) {
            wasm_valtype_vec_t param_vec;
            wasm_valtype_vec_t result_vec;
            wasm_valtype_vec_new_uninitialized(&param_vec, params.size());
            wasm_valtype_vec_new_uninitialized(&result_vec, results.size());
            size_t i = 0;

            for (auto val : params) {
                param_vec.data[i++] = val.ptr.release(); // NOLINT
            }
            i = 0;
            for (auto val : results) {
                result_vec.data[i++] = val.ptr.release(); // NOLINT
            }

            return wasm_functype_new(&param_vec, &result_vec);
        }

        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator->() { return &ref; }
        /// \brief Returns the underlying `Ref`, a non-owning reference pointing to
        /// this instance.
        Ref *operator*() { return &ref; }
    };

/**
 * \brief Type information about a WebAssembly import.
 */
    class ImportType {
    public:
        /// Non-owning reference to an `ImportType`, must not be used after the
        /// original owner is deleted.
        class Ref {
            friend class ExternType;

            const wasm_importtype_t *ptr;

            // TODO: can this circle be broken another way?
            const wasm_externtype_t *raw_type() { return wasm_importtype_type(ptr); }

        public:
            /// Creates a new reference from the raw underlying C API representation.
            Ref(const wasm_importtype_t *ptr) : ptr(ptr) {}

            /// Returns the module name associated with this import.
            std::string_view module() {
                const auto *name = wasm_importtype_module(ptr);
                return std::string_view(name->data, name->size);
            }

            /// Returns the field name associated with this import.
            std::string_view name() {
                const auto *name = wasm_importtype_name(ptr);
                return std::string_view(name->data, name->size);
            }
        };

        /// An owned list of `ImportType` instances.
        class List {
            friend class Module;
            wasm_importtype_vec_t list;

        public:
            /// Creates an empty list
            List() : list{} {
                list.size = 0;
                list.data = nullptr;
            }
            List(const List &other) = delete;
            /// Moves another list into this one.
            List(List &&other) noexcept : list(other.list) { other.list.size = 0; }
            ~List() {
                if (list.size > 0) {
                    wasm_importtype_vec_delete(&list);
                }
            }

            List &operator=(const List &other) = delete;
            /// Moves another list into this one.
            List &operator=(List &&other) noexcept {
                std::swap(list, other.list);
                return *this;
            }

            /// Iterator type, which is a list of non-owning `ImportType::Ref`
            /// instances.
            typedef const Ref *iterator;
            /// Returns the start of iteration.
            iterator begin() const {
                return reinterpret_cast<iterator>(&list.data[0]); // NOLINT
            }
            /// Returns the end of iteration.
            iterator end() const {
                return reinterpret_cast<iterator>(&list.data[list.size]); // NOLINT
            }
            /// Returns the size of this list.
            size_t size() const { return list.size; }
        };
    };

/**
 * \brief Type information about a WebAssembly export
 */
    class ExportType {

    public:
        /// \brief Non-owning reference to an `ExportType`.
        ///
        /// Note to get type information you can use `ExternType::from_export`.
        class Ref {
            friend class ExternType;

            const wasm_exporttype_t *ptr;

            const wasm_externtype_t *raw_type() { return wasm_exporttype_type(ptr); }

        public:
            /// Creates a new reference from the raw underlying C API representation.
            Ref(const wasm_exporttype_t *ptr) : ptr(ptr) {}

            /// Returns the name of this export.
            std::string_view name() {
                const auto *name = wasm_exporttype_name(ptr);
                return std::string_view(name->data, name->size);
            }
        };

        /// An owned list of `ExportType` instances.
        class List {
            friend class Module;
            wasm_exporttype_vec_t list;

        public:
            /// Creates an empty list
            List() : list{} {
                list.size = 0;
                list.data = nullptr;
            }
            List(const List &other) = delete;
            /// Moves another list into this one.
            List(List &&other) noexcept : list(other.list) { other.list.size = 0; }
            ~List() {
                if (list.size > 0) {
                    wasm_exporttype_vec_delete(&list);
                }
            }

            List &operator=(const List &other) = delete;
            /// Moves another list into this one.
            List &operator=(List &&other) noexcept {
                std::swap(list, other.list);
                return *this;
            }

            /// Iterator type, which is a list of non-owning `ExportType::Ref`
            /// instances.
            typedef const Ref *iterator;
            /// Returns the start of iteration.
            iterator begin() const {
                return reinterpret_cast<iterator>(&list.data[0]); // NOLINT
            }
            /// Returns the end of iteration.
            iterator end() const {
                return reinterpret_cast<iterator>(&list.data[list.size]); // NOLINT
            }
            /// Returns the size of this list.
            size_t size() const { return list.size; }
        };
    };

/**
 * \brief Generic type of a WebAssembly item.
 */
    class ExternType {
        friend class ExportType;
        friend class ImportType;

    public:
        /// \typedef Ref
        /// \brief Non-owning reference to an item's type
        ///
        /// This cannot be used after the original owner has been deleted, and
        /// otherwise this is used to determine what the actual type of the outer item
        /// is.
        typedef std::variant<FuncType::Ref, GlobalType::Ref, TableType::Ref,
                MemoryType::Ref>
                Ref;

        /// Extract the type of the item imported by the provided type.
        static Ref from_import(ImportType::Ref ty) {
            // TODO: this would ideally be some sort of implicit constructor, unsure how
            // to do that though...
            return ref_from_c(ty.raw_type());
        }

        /// Extract the type of the item exported by the provided type.
        static Ref from_export(ExportType::Ref ty) {
            // TODO: this would ideally be some sort of implicit constructor, unsure how
            // to do that though...
            return ref_from_c(ty.raw_type());
        }

    private:
        static Ref ref_from_c(const wasm_externtype_t *ptr) {
            switch (wasm_externtype_kind(ptr)) {
                case WASM_EXTERN_FUNC:
                    return wasm_externtype_as_functype_const(ptr);
                case WASM_EXTERN_GLOBAL:
                    return wasm_externtype_as_globaltype_const(ptr);
                case WASM_EXTERN_TABLE:
                    return wasm_externtype_as_tabletype_const(ptr);
                case WASM_EXTERN_MEMORY:
                    return wasm_externtype_as_memorytype_const(ptr);
            }
            std::abort();
        }
    };

/**
 * \brief Non-owning reference to a WebAssembly function frame as part of a
 * `Trace`
 *
 * A `FrameRef` represents a WebAssembly function frame on the stack which was
 * collected as part of a trap.
 */
    class FrameRef {
        wasm_frame_t *frame;

    public:
        /// Returns the WebAssembly function index of this function, in the original
        /// module.
        uint32_t func_index() const { return wasm_frame_func_index(frame); }
        /// Returns the offset, in bytes from the start of the function in the
        /// original module, to this frame's program counter.
        size_t func_offset() const { return wasm_frame_func_offset(frame); }
        /// Returns the offset, in bytes from the start of the original module,
        /// to this frame's program counter.
        size_t module_offset() const { return wasm_frame_module_offset(frame); }

        /// Returns the name, if present, associated with this function.
        ///
        /// Note that this requires that the `name` section is present in the original
        /// WebAssembly binary.
        std::optional<std::string_view> func_name() const {
            const auto *name = wasmtime_frame_func_name(frame);
            if (name != nullptr) {
                return std::string_view(name->data, name->size);
            }
            return std::nullopt;
        }

        /// Returns the name, if present, associated with this function's module.
        ///
        /// Note that this requires that the `name` section is present in the original
        /// WebAssembly binary.
        std::optional<std::string_view> module_name() const {
            const auto *name = wasmtime_frame_module_name(frame);
            if (name != nullptr) {
                return std::string_view(name->data, name->size);
            }
            return std::nullopt;
        }
    };

/**
 * \brief An owned vector of `FrameRef` instances representing the WebAssembly
 * call-stack on a trap.
 *
 * This can be used to iterate over the frames of a trap and determine what was
 * running when a trap happened.
 */
    class Trace {
        friend class Trap;

        wasm_frame_vec_t vec;

        Trace(wasm_frame_vec_t vec) : vec(vec) {}

    public:
        ~Trace() { wasm_frame_vec_delete(&vec); }

        Trace(const Trace &other) = delete;
        Trace(Trace &&other) = delete;
        Trace &operator=(const Trace &other) = delete;
        Trace &operator=(Trace &&other) = delete;

        /// Iterator used to iterate over this trace.
        typedef const FrameRef *iterator;

        /// Returns the start of iteration
        iterator begin() const {
            return reinterpret_cast<FrameRef *>(&vec.data[0]); // NOLINT
        }
        /// Returns the end of iteration
        iterator end() const {
            return reinterpret_cast<FrameRef *>(&vec.data[vec.size]); // NOLINT
        }
        /// Returns the size of this trace, or how many frames it contains.
        size_t size() const { return vec.size; }
    };

/**
 * \brief Information about a WebAssembly trap.
 *
 * Traps can happen during normal wasm execution (such as the `unreachable`
 * instruction) but they can also happen in host-provided functions to a host
 * function can simulate raising a trap.
 *
 * Traps have a message associated with them as well as a trace of WebAssembly
 * frames on the stack.
 */
    class Trap {
        friend class Linker;
        friend class Instance;
        friend class Func;
        template <typename Params, typename Results> friend class TypedFunc;

        struct deleter {
            void operator()(wasm_trap_t *p) const { wasm_trap_delete(p); }
        };

        std::unique_ptr<wasm_trap_t, deleter> ptr;

        Trap(wasm_trap_t *ptr) : ptr(ptr) {}

    public:
        /// Creates a new host-defined trap with the specified message.
        explicit Trap(std::string_view msg)
                : Trap(wasmtime_trap_new(msg.data(), msg.size())) {}

        /// Returns the descriptive message associated with this trap
        std::string message() const {
            wasm_byte_vec_t msg;
            wasm_trap_message(ptr.get(), &msg);
            std::string ret(msg.data, msg.size - 1);
            wasm_byte_vec_delete(&msg);
            return ret;
        }

        /// If this trap represents a call to `exit` for WASI, this will return the
        /// optional error code associated with the exit trap.
        std::optional<int32_t> i32_exit() const {
            int32_t status = 0;
            if (wasmtime_trap_exit_status(ptr.get(), &status)) {
                return status;
            }
            return std::nullopt;
        }

        /// Returns the trace of WebAssembly frames associated with this trap.
        Trace trace() const {
            wasm_frame_vec_t frames;
            wasm_trap_trace(ptr.get(), &frames);
            return Trace(frames);
        }
    };

/// Structure used to represent either a `Trap` or an `Error`.
    struct TrapError {
        /// Storage for what this trap represents.
        std::variant<Trap, Error> data;

        /// Creates a new `TrapError` from a `Trap`
        TrapError(Trap t) : data(std::move(t)) {}
        /// Creates a new `TrapError` from an `Error`
        TrapError(Error e) : data(std::move(e)) {}

        /// Dispatches internally to return the message associated with this error.
        std::string message() const {
            if (const auto *trap = std::get_if<Trap>(&data)) {
                return trap->message();
            }
            if (const auto *error = std::get_if<Error>(&data)) {
                return std::string(error->message());
            }
            std::abort();
        }
    };

/// Result used by functions which can fail because of invariants being violated
/// (such as a type error) as well as because of a WebAssembly trap.
    template <typename T> using TrapResult = Result<T, TrapError>;

/**
 * \brief Representation of a compiled WebAssembly module.
 *
 * This type contains JIT code of a compiled WebAssembly module. A `Module` is
 * connected to an `Engine` and can only be instantiated within that `Engine`.
 * You can inspect a `Module` for its type information. This is passed as an
 * argument to other APIs to instantiate it.
 */
    class Module {
        friend class Store;
        friend class Instance;
        friend class Linker;

        struct deleter {
            void operator()(wasmtime_module_t *p) const { wasmtime_module_delete(p); }
        };

        std::unique_ptr<wasmtime_module_t, deleter> ptr;

        Module(wasmtime_module_t *raw) : ptr(raw) {}

    public:
        /// Copies another module into this one.
        Module(const Module &other) : ptr(wasmtime_module_clone(other.ptr.get())) {}
        /// Copies another module into this one.
        Module &operator=(const Module &other) {
            ptr.reset(wasmtime_module_clone(other.ptr.get()));
            return *this;
        }
        ~Module() = default;
        /// Moves resources from another module into this one.
        Module(Module &&other) = default;
        /// Moves resources from another module into this one.
        Module &operator=(Module &&other) = default;

        /**
         * \brief Compiles a module from the WebAssembly text format.
         *
         * This function will automatically use `wat2wasm` on the input and then
         * delegate to the #compile function.
         */
        [[nodiscard]] static Result<Module> compile(Engine &engine,
                                                    std::string_view wat) {
            auto wasm = wat2wasm(wat);
            if (!wasm) {
                return wasm.err();
            }
            auto bytes = wasm.ok();
            return compile(engine, bytes);
        }

        /**
         * \brief Compiles a module from the WebAssembly binary format.
         *
         * This function compiles the provided WebAssembly binary specified by `wasm`
         * within the compilation settings configured by `engine`. This method is
         * synchronous and will not return until the module has finished compiling.
         *
         * This function can fail if the WebAssembly binary is invalid or doesn't
         * validate (or similar).
         */
        [[nodiscard]] static Result<Module> compile(Engine &engine,
                                                    Span<uint8_t> wasm) {
            wasmtime_module_t *ret = nullptr;
            auto *error =
                    wasmtime_module_new(engine.ptr.get(), wasm.data(), wasm.size(), &ret);
            if (error != nullptr) {
                return Error(error);
            }
            return Module(ret);
        }

        /**
         * \brief Validates the provided WebAssembly binary without compiling it.
         *
         * This function will validate whether the provided binary is indeed valid
         * within the compilation settings of the `engine` provided.
         */
        [[nodiscard]] static Result<std::monostate> validate(Engine &engine,
                                                             Span<uint8_t> wasm) {
            auto *error =
                    wasmtime_module_validate(engine.ptr.get(), wasm.data(), wasm.size());
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /**
         * \brief Deserializes a previous list of bytes created with `serialize`.
         *
         * This function is intended to be much faster than `compile` where it uses
         * the artifacts of a previous compilation to quickly create an in-memory
         * module ready for instantiation.
         *
         * It is not safe to pass arbitrary input to this function, it is only safe to
         * pass in output from previous calls to `serialize`. For more information see
         * the Rust documentation -
         * https://docs.wasmtime.dev/api/wasmtime/struct.Module.html#method.deserialize
         */
        [[nodiscard]] static Result<Module> deserialize(Engine &engine,
                                                        Span<uint8_t> wasm) {
            wasmtime_module_t *ret = nullptr;
            auto *error = wasmtime_module_deserialize(engine.ptr.get(), wasm.data(),
                                                      wasm.size(), &ret);
            if (error != nullptr) {
                return Error(error);
            }
            return Module(ret);
        }

        /**
         * \brief Deserializes a module from an on-disk file.
         *
         * This function is the same as `deserialize` except that it reads the data
         * for the serialized module from the path on disk. This can be faster than
         * the alternative which may require copying the data around.
         *
         * It is not safe to pass arbitrary input to this function, it is only safe to
         * pass in output from previous calls to `serialize`. For more information see
         * the Rust documentation -
         * https://docs.wasmtime.dev/api/wasmtime/struct.Module.html#method.deserialize
         */
        [[nodiscard]] static Result<Module>
        deserialize_file(Engine &engine, const std::string &path) {
            wasmtime_module_t *ret = nullptr;
            auto *error =
                    wasmtime_module_deserialize_file(engine.ptr.get(), path.c_str(), &ret);
            if (error != nullptr) {
                return Error(error);
            }
            return Module(ret);
        }

        /// Returns the list of types imported by this module.
        ImportType::List imports() const {
            ImportType::List list;
            wasmtime_module_imports(ptr.get(), &list.list);
            return list;
        }

        /// Returns the list of types exported by this module.
        ExportType::List exports() const {
            ExportType::List list;
            wasmtime_module_exports(ptr.get(), &list.list);
            return list;
        }

        /**
         * \brief Serializes this module to a list of bytes.
         *
         * The returned bytes can then be used to later pass to `deserialize` to
         * quickly recreate this module in a different process perhaps.
         */
        [[nodiscard]] Result<std::vector<uint8_t>> serialize() const {
            wasm_byte_vec_t bytes;
            auto *error = wasmtime_module_serialize(ptr.get(), &bytes);
            if (error != nullptr) {
                return Error(error);
            }
            std::vector<uint8_t> ret;
            // NOLINTNEXTLINE TODO can this be done without triggering lints?
            Span<uint8_t> raw(reinterpret_cast<uint8_t *>(bytes.data), bytes.size);
            ret.assign(raw.begin(), raw.end());
            wasm_byte_vec_delete(&bytes);
            return ret;
        }
    };

/**
 * \brief Configuration for an instance of WASI.
 *
 * This is inserted into a store with `Store::Context::set_wasi`.
 */
    class WasiConfig {
        friend class Store;

        struct deleter {
            void operator()(wasi_config_t *p) const { wasi_config_delete(p); }
        };

        std::unique_ptr<wasi_config_t, deleter> ptr;

    public:
        /// Creates a new configuration object with default settings.
        WasiConfig() : ptr(wasi_config_new()) {}

        /// Configures the argv explicitly with the given string array.
        void argv(const std::vector<std::string> &args) {
            std::vector<const char *> ptrs;
            ptrs.reserve(args.size());
            for (const auto &arg : args) {
                ptrs.push_back(arg.c_str());
            }

            wasi_config_set_argv(ptr.get(), (int)args.size(), ptrs.data());
        }

        /// Configures the argv for wasm to be inherited from this process itself.
        void inherit_argv() { wasi_config_inherit_argv(ptr.get()); }

        /// Configures the environment variables available to wasm, specified here as
        /// a list of pairs where the first element of the pair is the key and the
        /// second element is the value.
        void env(const std::vector<std::pair<std::string, std::string>> &env) {
            std::vector<const char *> names;
            std::vector<const char *> values;
            for (const auto &[name, value] : env) {
                names.push_back(name.c_str());
                values.push_back(value.c_str());
            }
            wasi_config_set_env(ptr.get(), (int)env.size(), names.data(),
                                values.data());
        }

        /// Indicates that the entire environment of this process should be inherited
        /// by the wasi configuration.
        void inherit_env() { wasi_config_inherit_env(ptr.get()); }

        /// Configures the provided file to be used for the stdin of this WASI
        /// configuration.
        [[nodiscard]] bool stdin_file(const std::string &path) {
            return wasi_config_set_stdin_file(ptr.get(), path.c_str());
        }

        /// Configures this WASI configuration to inherit its stdin from the host
        /// process.
        void inherit_stdin() { return wasi_config_inherit_stdin(ptr.get()); }

        /// Configures the provided file to be created and all stdout output will be
        /// written there.
        [[nodiscard]] bool stdout_file(const std::string &path) {
            return wasi_config_set_stdout_file(ptr.get(), path.c_str());
        }

        /// Configures this WASI configuration to inherit its stdout from the host
        /// process.
        void inherit_stdout() { return wasi_config_inherit_stdout(ptr.get()); }

        /// Configures the provided file to be created and all stderr output will be
        /// written there.
        [[nodiscard]] bool stderr_file(const std::string &path) {
            return wasi_config_set_stderr_file(ptr.get(), path.c_str());
        }

        /// Configures this WASI configuration to inherit its stdout from the host
        /// process.
        void inherit_stderr() { return wasi_config_inherit_stderr(ptr.get()); }

        /// Opens `path` to be opened as `guest_path` in the WASI pseudo-filesystem.
        [[nodiscard]] bool preopen_dir(const std::string &path,
                                       const std::string &guest_path) {
            return wasi_config_preopen_dir(ptr.get(), path.c_str(), guest_path.c_str());
        }
    };

    class Caller;
    template <typename Params, typename Results> class TypedFunc;

/**
 * \brief Owner of all WebAssembly objects
 *
 * A `Store` owns all WebAssembly objects such as instances, globals, functions,
 * memories, etc. A `Store` is one of the main central points about working with
 * WebAssembly since it's an argument to almost all APIs. The `Store` serves as
 * a form of "context" to give meaning to the pointers of `Func` and friends.
 *
 * A `Store` can be sent between threads but it cannot generally be shared
 * concurrently between threads. Memory associated with WebAssembly instances
 * will be deallocated when the `Store` is deallocated.
 */
    class Store {
        struct deleter {
            void operator()(wasmtime_store_t *p) const { wasmtime_store_delete(p); }
        };

        std::unique_ptr<wasmtime_store_t, deleter> ptr;

        static void finalizer(void *ptr) {
            std::unique_ptr<std::any> _ptr(static_cast<std::any *>(ptr));
        }

    public:
        /// Creates a new `Store` within the provided `Engine`.
        explicit Store(Engine &engine)
                : ptr(wasmtime_store_new(engine.ptr.get(), nullptr, finalizer)) {}

        /**
         * \brief An interior pointer into a `Store`.
         *
         * A `Context` object is created from either a `Store` or a `Caller`. It is an
         * interior pointer into a `Store` and cannot be used outside the lifetime of
         * the original object it was created from.
         *
         * This object is an argument to most APIs in Wasmtime but typically doesn't
         * need to be constructed explicitly since it can be created from a `Store&`
         * or a `Caller&`.
         */
        class Context {
            friend class Global;
            friend class Table;
            friend class Memory;
            friend class Func;
            friend class Instance;
            friend class Linker;
            wasmtime_context_t *ptr;

            Context(wasmtime_context_t *ptr) : ptr(ptr) {}

        public:
            /// Creates a context referencing the provided `Store`.
            Context(Store &store) : Context(wasmtime_store_context(store.ptr.get())) {}
            /// Creates a context referencing the provided `Store`.
            Context(Store *store) : Context(*store) {}
            /// Creates a context referencing the provided `Caller`.
            Context(Caller &caller);
            /// Creates a context referencing the provided `Caller`.
            Context(Caller *caller);

            /// Runs a garbage collection pass in the referenced store to collect loose
            /// `externref` values, if any are available.
            void gc() { wasmtime_context_gc(ptr); }

            /// Injects fuel to be consumed within this store.
            ///
            /// Stores start with 0 fuel and if `Config::consume_fuel` is enabled then
            /// this is required if you want to let WebAssembly actually execute.
            ///
            /// Returns an error if fuel consumption isn't enabled.
            [[nodiscard]] Result<std::monostate> add_fuel(uint64_t fuel) {
                auto *error = wasmtime_context_add_fuel(ptr, fuel);
                if (error != nullptr) {
                    return Error(error);
                }
                return std::monostate();
            }

            /// Returns the amount of fuel consumed so far by executing WebAssembly.
            ///
            /// Returns `std::nullopt` if fuel consumption is not enabled.
            std::optional<uint64_t> fuel_consumed() const {
                uint64_t fuel = 0;
                if (wasmtime_context_fuel_consumed(ptr, &fuel)) {
                    return fuel;
                }
                return std::nullopt;
            }

            /// Set user specified data associated with this store.
            void set_data(std::any data) const {
                finalizer(static_cast<std::any *>(wasmtime_context_get_data(ptr)));
                wasmtime_context_set_data(
                        ptr, std::make_unique<std::any>(std::move(data)).release());
            }

            /// Get user specified data associated with this store.
            std::any &get_data() const {
                return *static_cast<std::any *>(wasmtime_context_get_data(ptr));
            }

            /// Configures the WASI state used by this store.
            ///
            /// This will only have an effect if used in conjunction with
            /// `Linker::define_wasi` because otherwise no host functions will use the
            /// WASI state.
            [[nodiscard]] Result<std::monostate> set_wasi(WasiConfig config) {
                auto *error = wasmtime_context_set_wasi(ptr, config.ptr.release());
                if (error != nullptr) {
                    return Error(error);
                }
                return std::monostate();
            }

            /// Configures this store's epoch deadline to be the specified number of
            /// ticks beyond the engine's current epoch.
            ///
            /// By default the deadline is the current engine's epoch, immediately
            /// interrupting code if epoch interruption is enabled. This must be called
            /// to extend the deadline to allow interruption.
            void set_epoch_deadline(uint64_t ticks_beyond_current) {
                wasmtime_context_set_epoch_deadline(ptr, ticks_beyond_current);
            }

            /// Returns the raw context pointer for the C API.
            wasmtime_context_t *raw_context() { return ptr; }
        };

        /// Explicit function to acquire a `Context` from this store.
        Context context() { return this; }
    };

/**
 * \brief Representation of a WebAssembly `externref` value.
 *
 * This class represents an value that cannot be forged by WebAssembly itself.
 * All `ExternRef` values are guaranteed to be created by the host and its
 * embedding. It's suitable to place private data structures in here which
 * WebAssembly will not have access to, only other host functions will have
 * access to them.
 */
    class ExternRef {
        friend class Val;

        struct deleter {
            void operator()(wasmtime_externref_t *p) const {
                wasmtime_externref_delete(p);
            }
        };

        std::unique_ptr<wasmtime_externref_t, deleter> ptr;

        static void finalizer(void *ptr) {
            std::unique_ptr<std::any> _ptr(static_cast<std::any *>(ptr));
        }

    public:
        /// Creates a new `ExternRef` from an owned C API raw pointer.
        explicit ExternRef(wasmtime_externref_t *ptr) : ptr(ptr) {}

        /// Creates a new `externref` value from the provided argument.
        ///
        /// Note that `val` should be safe to send across threads and should own any
        /// memory that it points to. Also note that `ExternRef` is similar to a
        /// `std::shared_ptr` in that there can be many references to the same value.
        template <typename T>
        explicit ExternRef(T val)
                : ExternRef(wasmtime_externref_new(
                std::make_unique<std::any>(std::move(val)).release(), finalizer)) {}
        /// Performs a shallow copy of another `externref` value, creating another
        /// reference to it.
        ExternRef(const ExternRef &other)
                : ExternRef(wasmtime_externref_clone(other.ptr.get())) {}
        /// Performs a shallow copy of another `externref` value, creating another
        /// reference to it.
        ExternRef &operator=(const ExternRef &other) {
            ptr.reset(wasmtime_externref_clone(other.ptr.get()));
            return *this;
        }
        /// Moves the resources pointed to by `other` into `this`.
        ExternRef(ExternRef &&other) = default;
        /// Moves the resources pointed to by `other` into `this`.
        ExternRef &operator=(ExternRef &&other) = default;
        ~ExternRef() = default;

        /// Returns the underlying host data associated with this `ExternRef`.
        std::any &data() {
            return *static_cast<std::any *>(wasmtime_externref_data(ptr.get()));
        }

        /// Returns the raw underlying C API pointer.
        ///
        /// This class still retains ownership of the pointer.
        wasmtime_externref_t *raw() const { return ptr.get(); }
    };

    class Func;
    class Global;
    class Instance;
    class Memory;
    class Table;

/// \typedef Extern
/// \brief Representation of an external WebAssembly item
    typedef std::variant<Func, Global, Memory, Table> Extern;

/// \brief Container for the `v128` WebAssembly type.
    struct V128 {
        /// \brief The little-endian bytes of the `v128` value.
        wasmtime_v128 v128;

        /// \brief Creates a new zero-value `v128`.
        V128() : v128{} { memset(&v128[0], 0, sizeof(wasmtime_v128)); }

        /// \brief Creates a new `V128` from its C API representation.
        V128(const wasmtime_v128 &v) : v128{} {
            memcpy(&v128[0], &v[0], sizeof(wasmtime_v128));
        }
    };

/**
 * \brief Representation of a generic WebAssembly value.
 *
 * This is roughly equivalent to a tagged union of all possible WebAssembly
 * values. This is later used as an argument with functions, globals, tables,
 * etc.
 */
    class Val {
        friend class Global;
        friend class Table;
        friend class Func;

        wasmtime_val_t val;

        Val() : val{} {
            val.kind = WASMTIME_I32;
            val.of.i32 = 0;
        }
        Val(wasmtime_val_t val) : val(val) {}

    public:
        /// Creates a new `i32` WebAssembly value.
        Val(int32_t i32) : val{} {
            val.kind = WASMTIME_I32;
            val.of.i32 = i32;
        }
        /// Creates a new `i64` WebAssembly value.
        Val(int64_t i64) : val{} {
            val.kind = WASMTIME_I64;
            val.of.i64 = i64;
        }
        /// Creates a new `f32` WebAssembly value.
        Val(float f32) : val{} {
            val.kind = WASMTIME_F32;
            val.of.f32 = f32;
        }
        /// Creates a new `f64` WebAssembly value.
        Val(double f64) : val{} {
            val.kind = WASMTIME_F64;
            val.of.f64 = f64;
        }
        /// Creates a new `v128` WebAssembly value.
        Val(const V128 &v128) : val{} {
            val.kind = WASMTIME_V128;
            memcpy(&val.of.v128[0], &v128.v128[0], sizeof(wasmtime_v128));
        }
        /// Creates a new `funcref` WebAssembly value.
        Val(std::optional<Func> func);
        /// Creates a new `funcref` WebAssembly value which is not `ref.null func`.
        Val(Func func);
        /// Creates a new `externref` value.
        Val(std::optional<ExternRef> ptr) : val{} {
            val.kind = WASMTIME_EXTERNREF;
            val.of.externref = nullptr;
            if (ptr) {
                val.of.externref = ptr->ptr.release();
            }
        }
        /// Creates a new `externref` WebAssembly value which is not `ref.null
        /// extern`.
        Val(ExternRef ptr);
        /// Copies the contents of another value into this one.
        Val(const Val &other) : val{} {
            val.kind = WASMTIME_I32;
            val.of.i32 = 0;
            wasmtime_val_copy(&val, &other.val);
        }
        /// Moves the resources from another value into this one.
        Val(Val &&other) noexcept : val{} {
            val.kind = WASMTIME_I32;
            val.of.i32 = 0;
            std::swap(val, other.val);
        }

        ~Val() {
            if (val.kind == WASMTIME_EXTERNREF && val.of.externref != nullptr) {
                wasmtime_externref_delete(val.of.externref);
            }
        }

        /// Copies the contents of another value into this one.
        Val &operator=(const Val &other) noexcept {
            if (val.kind == WASMTIME_EXTERNREF && val.of.externref != nullptr) {
                wasmtime_externref_delete(val.of.externref);
            }
            wasmtime_val_copy(&val, &other.val);
            return *this;
        }
        /// Moves the resources from another value into this one.
        Val &operator=(Val &&other) noexcept {
            std::swap(val, other.val);
            return *this;
        }

        /// Returns the kind of value that this value has.
        ValKind kind() const {
            switch (val.kind) {
                case WASMTIME_I32:
                    return ValKind::I32;
                case WASMTIME_I64:
                    return ValKind::I64;
                case WASMTIME_F32:
                    return ValKind::F32;
                case WASMTIME_F64:
                    return ValKind::F64;
                case WASMTIME_FUNCREF:
                    return ValKind::FuncRef;
                case WASMTIME_EXTERNREF:
                    return ValKind::ExternRef;
                case WASMTIME_V128:
                    return ValKind::V128;
            }
            std::abort();
        }

        /// Returns the underlying `i32`, requires `kind() == KindI32` or aborts the
        /// process.
        int32_t i32() const {
            if (val.kind != WASMTIME_I32) {
                std::abort();
            }
            return val.of.i32;
        }

        /// Returns the underlying `i64`, requires `kind() == KindI64` or aborts the
        /// process.
        int64_t i64() const {
            if (val.kind != WASMTIME_I64) {
                std::abort();
            }
            return val.of.i64;
        }

        /// Returns the underlying `f32`, requires `kind() == KindF32` or aborts the
        /// process.
        float f32() const {
            if (val.kind != WASMTIME_F32) {
                std::abort();
            }
            return val.of.f32;
        }

        /// Returns the underlying `f64`, requires `kind() == KindF64` or aborts the
        /// process.
        double f64() const {
            if (val.kind != WASMTIME_F64) {
                std::abort();
            }
            return val.of.f64;
        }

        /// Returns the underlying `v128`, requires `kind() == KindV128` or aborts
        /// the process.
        V128 v128() const {
            if (val.kind != WASMTIME_V128) {
                std::abort();
            }
            return val.of.v128;
        }

        /// Returns the underlying `externref`, requires `kind() == KindExternRef` or
        /// aborts the process.
        ///
        /// Note that `externref` is a nullable reference, hence the `optional` return
        /// value.
        std::optional<ExternRef> externref() const {
            if (val.kind != WASMTIME_EXTERNREF) {
                std::abort();
            }
            if (val.of.externref != nullptr) {
                return ExternRef(wasmtime_externref_clone(val.of.externref));
            }
            return std::nullopt;
        }

        /// Returns the underlying `funcref`, requires `kind() == KindFuncRef` or
        /// aborts the process.
        ///
        /// Note that `funcref` is a nullable reference, hence the `optional` return
        /// value.
        std::optional<Func> funcref() const;
    };

/**
 * \brief Structure provided to host functions to lookup caller information or
 * acquire a `Store::Context`.
 *
 * This structure is passed to all host functions created with `Func`. It can be
 * used to create a `Store::Context`.
 */
    class Caller {
        friend class Func;
        friend class Store;
        wasmtime_caller_t *ptr;
        Caller(wasmtime_caller_t *ptr) : ptr(ptr) {}

    public:
        /// Attempts to load an exported item from the calling instance.
        ///
        /// For more information see the Rust documentation -
        /// https://docs.wasmtime.dev/api/wasmtime/struct.Caller.html#method.get_export
        std::optional<Extern> get_export(std::string_view name);

        /// Explicitly acquire a `Store::Context` from this `Caller`.
        Store::Context context() { return this; }
    };

    inline Store::Context::Context(Caller &caller)
            : Context(wasmtime_caller_context(caller.ptr)) {}
    inline Store::Context::Context(Caller *caller) : Context(*caller) {}

    namespace detail {

/// A "trait" for native types that correspond to WebAssembly types for use with
/// `Func::wrap` and `TypedFunc::call`
        template <typename T> struct WasmType { static const bool valid = false; };

/// Helper macro to define `WasmType` definitions for primitive types like
/// int32_t and such.
// NOLINTNEXTLINE
#define NATIVE_WASM_TYPE(native, valkind, field)                               \
  template <> struct WasmType<native> {                                        \
    static const bool valid = true;                                            \
    static const ValKind kind = ValKind::valkind;                              \
    static void store(Store::Context cx, wasmtime_val_raw_t *p,                \
                      const native &t) {                                       \
      p->field = t;                                                            \
    }                                                                          \
    static native load(Store::Context cx, wasmtime_val_raw_t *p) {             \
      return p->field;                                                         \
    }                                                                          \
  };

        NATIVE_WASM_TYPE(int32_t, I32, i32)
        NATIVE_WASM_TYPE(uint32_t, I32, i32)
        NATIVE_WASM_TYPE(int64_t, I64, i64)
        NATIVE_WASM_TYPE(uint64_t, I64, i64)
        NATIVE_WASM_TYPE(float, F32, f32)
        NATIVE_WASM_TYPE(double, F64, f64)

#undef NATIVE_WASM_TYPE

/// Type information for `externref`, represented on the host as an optional
/// `ExternRef`.
        template <> struct WasmType<std::optional<ExternRef>> {
            static const bool valid = true;
            static const ValKind kind = ValKind::ExternRef;
            static void store(Store::Context cx, wasmtime_val_raw_t *p,
                              const std::optional<ExternRef> &ref) {
                if (ref) {
                    p->externref = wasmtime_externref_to_raw(cx.raw_context(), ref->raw());
                } else {
                    p->externref = 0;
                }
            }
            static std::optional<ExternRef> load(Store::Context cx,
                                                 wasmtime_val_raw_t *p) {
                if (p->externref == 0) {
                    return std::nullopt;
                }
                return ExternRef(
                        wasmtime_externref_from_raw(cx.raw_context(), p->externref));
            }
        };

/// Type information for the `V128` host value used as a wasm value.
        template <> struct WasmType<V128> {
            static const bool valid = true;
            static const ValKind kind = ValKind::V128;
            static void store(Store::Context cx, wasmtime_val_raw_t *p, const V128 &t) {
                memcpy(&p->v128[0], &t.v128[0], sizeof(wasmtime_v128));
            }
            static V128 load(Store::Context cx, wasmtime_val_raw_t *p) { return p->v128; }
        };

/// A "trait" for a list of types and operations on them, used for `Func::wrap`
/// and `TypedFunc::call`
///
/// The base case is a single type which is a list of one element.
        template <typename T> struct WasmTypeList {
            static const bool valid = WasmType<T>::valid;
            static const size_t size = 1;
            static bool matches(ValType::ListRef types) {
                return WasmTypeList<std::tuple<T>>::matches(types);
            }
            static void store(Store::Context cx, wasmtime_val_raw_t *storage,
                              const T &t) {
                WasmType<T>::store(cx, storage, t);
            }
            static T load(Store::Context cx, wasmtime_val_raw_t *storage) {
                return WasmType<T>::load(cx, storage);
            }
            static std::vector<ValType> types() { return {WasmType<T>::kind}; }
        };

/// std::monostate translates to an empty list of types.
        template <> struct WasmTypeList<std::monostate> {
            static const bool valid = true;
            static const size_t size = 0;
            static bool matches(ValType::ListRef types) { return types.size() == 0; }
            static void store(Store::Context cx, wasmtime_val_raw_t *storage,
                              const std::monostate &t) {}
            static std::monostate load(Store::Context cx, wasmtime_val_raw_t *storage) {
                return std::monostate();
            }
            static std::vector<ValType> types() { return {}; }
        };

/// std::tuple<> translates to the corresponding list of types
        template <typename... T> struct WasmTypeList<std::tuple<T...>> {
            static const bool valid = (WasmType<T>::valid && ...);
            static const size_t size = sizeof...(T);
            static bool matches(ValType::ListRef types) {
                if (types.size() != size) {
                    return false;
                }
                size_t n = 0;
                return ((WasmType<T>::kind == types.begin()[n++].kind()) && ...);
            }
            static void store(Store::Context cx, wasmtime_val_raw_t *storage,
                              const std::tuple<T...> &t) {
                size_t n = 0;
                std::apply(
                        [&](const auto &...val) {
                            (WasmType<T>::store(cx, &storage[n++], val), ...); // NOLINT
                        },
                        t);
            }
            static std::tuple<T...> load(Store::Context cx, wasmtime_val_raw_t *storage) {
                size_t n = 0;
                return std::tuple<T...>{WasmType<T>::load(cx, &storage[n++])...}; // NOLINT
            }
            static std::vector<ValType> types() { return {WasmType<T>::kind...}; }
        };

/// A "trait" for what can be returned from closures specified to `Func::wrap`.
///
/// The base case here is a bare return value like `int32_t`.
        template <typename R> struct WasmHostRet {
            using Results = WasmTypeList<R>;

            template <typename F, typename... A>
            static std::optional<Trap> invoke(F f, Caller cx, wasmtime_val_raw_t *raw,
                                              A... args) {
                auto ret = f(args...);
                Results::store(cx, raw, ret);
                return std::nullopt;
            }
        };

/// Host functions can return nothing
        template <> struct WasmHostRet<void> {
            using Results = WasmTypeList<std::tuple<>>;

            template <typename F, typename... A>
            static std::optional<Trap> invoke(F f, Caller cx, wasmtime_val_raw_t *raw,
                                              A... args) {
                f(args...);
                return std::nullopt;
            }
        };

// Alternative method of returning "nothing" (also enables `std::monostate` in
// the `R` type of `Result` below)
        template <> struct WasmHostRet<std::monostate> : public WasmHostRet<void> {};

/// Host functions can return a result which allows them to also possibly return
/// a trap.
        template <typename R> struct WasmHostRet<Result<R, Trap>> {
            using Results = WasmTypeList<R>;

            template <typename F, typename... A>
            static std::optional<Trap> invoke(F f, Caller cx, wasmtime_val_raw_t *raw,
                                              A... args) {
                Result<R, Trap> ret = f(args...);
                if (!ret) {
                    return ret.err();
                }
                Results::store(cx, raw, ret.ok());
                return std::nullopt;
            }
        };

        template <typename F, typename = void> struct WasmHostFunc;

/// Base type information for host function pointers being used as wasm
/// functions
        template <typename R, typename... A> struct WasmHostFunc<R (*)(A...)> {
            using Params = WasmTypeList<std::tuple<A...>>;
            using Results = typename WasmHostRet<R>::Results;

            template <typename F>
            static std::optional<Trap> invoke(F &f, Caller cx, wasmtime_val_raw_t *raw) {
                auto params = Params::load(cx, raw);
                return std::apply(
                        [&](const auto &...val) {
                            return WasmHostRet<R>::invoke(f, cx, raw, val...);
                        },
                        params);
            }
        };

/// Function type information, but with a `Caller` first parameter
        template <typename R, typename... A>
        struct WasmHostFunc<R (*)(Caller, A...)> : public WasmHostFunc<R (*)(A...)> {
            // Override `invoke` here to pass the `cx` as the first parameter
            template <typename F>
            static std::optional<Trap> invoke(F &f, Caller cx, wasmtime_val_raw_t *raw) {
                auto params = WasmTypeList<std::tuple<A...>>::load(cx, raw);
                return std::apply(
                        [&](const auto &...val) {
                            return WasmHostRet<R>::invoke(f, cx, raw, cx, val...);
                        },
                        params);
            }
        };

/// Function type information, but with as a host method.
        template <typename R, typename C, typename... A>
        struct WasmHostFunc<R (C::*)(A...)> : public WasmHostFunc<R (*)(A...)> {};

/// Function type information, but with as a const host method.
        template <typename R, typename C, typename... A>
        struct WasmHostFunc<R (C::*)(A...) const> : public WasmHostFunc<R (*)(A...)> {};

/// Function type information, but with as a host method with a `Caller` first
/// parameter.
        template <typename R, typename C, typename... A>
        struct WasmHostFunc<R (C::*)(Caller, A...)>
                : public WasmHostFunc<R (*)(Caller, A...)> {};

/// Function type information, but with as a host const method with a `Caller`
/// first parameter.
        template <typename R, typename C, typename... A>
        struct WasmHostFunc<R (C::*)(Caller, A...) const>
                : public WasmHostFunc<R (*)(Caller, A...)> {};

// Forward... something? Not entirely sure but this makes things work.
        template <typename T>
        struct WasmHostFunc<T, std::void_t<decltype(&T::operator())>>
                : public WasmHostFunc<decltype(&T::operator())> {};

    } // namespace detail

    using namespace detail;

/**
 * \brief Representation of a WebAssembly function.
 *
 * This class represents a WebAssembly function, either created through
 * instantiating a module or a host function.
 *
 * Note that this type does not itself own any resources. It points to resources
 * owned within a `Store` and the `Store` must be passed in as the first
 * argument to the functions defined on `Func`. Note that if the wrong `Store`
 * is passed in then the process will be aborted.
 */
    class Func {
        friend class Val;
        friend class Instance;
        friend class Linker;
        template <typename Params, typename Results> friend class TypedFunc;

        wasmtime_func_t func;

        template <typename F>
        static wasm_trap_t *raw_callback(void *env, wasmtime_caller_t *caller,
                                         const wasmtime_val_t *args, size_t nargs,
                                         wasmtime_val_t *results, size_t nresults) {
            F *func = reinterpret_cast<F *>(env);                          // NOLINT
            Span<const Val> args_span(reinterpret_cast<const Val *>(args), // NOLINT
                                      nargs);
            Span<Val> results_span(reinterpret_cast<Val *>(results), // NOLINT
                                   nresults);
            Result<std::monostate, Trap> result =
                    (*func)(Caller(caller), args_span, results_span);
            if (!result) {
                return result.err().ptr.release();
            }
            return nullptr;
        }

        template <typename F>
        static wasm_trap_t *
        raw_callback_unchecked(void *env, wasmtime_caller_t *caller,
                               wasmtime_val_raw_t *args_and_results,
                               size_t nargs_and_results) {
            using HostFunc = WasmHostFunc<F>;
            Caller cx(caller);
            F *func = reinterpret_cast<F *>(env); // NOLINT
            auto trap = HostFunc::invoke(*func, cx, args_and_results);
            if (trap) {
                return trap->ptr.release();
            }
            return nullptr;
        }

        template <typename F> static void raw_finalize(void *env) {
            std::unique_ptr<F> ptr(reinterpret_cast<F *>(env)); // NOLINT
        }

    public:
        /// Creates a new function from the raw underlying C API representation.
        Func(wasmtime_func_t func) : func(func) {}

        /**
         * \brief Creates a new host-defined function.
         *
         * This constructor is used to create a host function within the store
         * provided. This is how WebAssembly can call into the host and make use of
         * external functionality.
         *
         * > **Note**: host functions created this way are more flexible but not
         * > as fast to call as those created by `Func::wrap`.
         *
         * \param cx the store to create the function within
         * \param ty the type of the function that will be created
         * \param f the host callback to be executed when this function is called.
         *
         * The parameter `f` is expected to be a lambda (or a lambda lookalike) which
         * takes three parameters:
         *
         * * The first parameter is a `Caller` to get recursive access to the store
         *   and other caller state.
         * * The second parameter is a `Span<const Val>` which is the list of
         *   parameters to the function. These parameters are guaranteed to be of the
         *   types specified by `ty` when constructing this function.
         * * The last argument is `Span<Val>` which is where to write the return
         *   values of the function. The function must produce the types of values
         *   specified by `ty` or otherwise a trap will be raised.
         *
         * The parameter `f` is expected to return `Result<std::monostate, Trap>`.
         * This allows `f` to raise a trap if desired, or otherwise return no trap and
         * finish successfully. If a trap is raised then the results pointer does not
         * need to be written to.
         */
        template <typename F,
                std::enable_if_t<
                        std::is_invocable_r_v<Result<std::monostate, Trap>, F, Caller,
                                Span<const Val>, Span<Val>>,
                        bool> = true>
        Func(Store::Context cx, const FuncType &ty, F f) : func({}) {
            wasmtime_func_new(cx.ptr, ty.ptr.get(), raw_callback<F>,
                              std::make_unique<F>(f).release(), raw_finalize<F>, &func);
        }

        /**
         * \brief Creates a new host function from the provided callback `f`,
         * inferring the WebAssembly function type from the host signature.
         *
         * This function is akin to the `Func` constructor except that the WebAssembly
         * type does not need to be specified and additionally the signature of `f`
         * is different. The main goal of this function is to enable WebAssembly to
         * call the function `f` as-fast-as-possible without having to validate any
         * types or such.
         *
         * The function `f` can optionally take a `Caller` as its first parameter,
         * but otherwise its arguments are translated to WebAssembly types:
         *
         * * `int32_t`, `uint32_t` - `i32`
         * * `int64_t`, `uint64_t` - `i64`
         * * `float` - `f32`
         * * `double` - `f64`
         * * `std::optional<Func>` - `funcref`
         * * `std::optional<ExternRef>` - `externref`
         * * `wasmtime::V128` - `v128`
         *
         * The function may only take these arguments and if it takes any other kinds
         * of arguments then it will fail to compile.
         *
         * The function may return a few different flavors of return values:
         *
         * * `void` - interpreted as returning nothing
         * * Any type above - interpreted as a singular return value.
         * * `std::tuple<T...>` where `T` is one of the valid argument types -
         *   interpreted as returning multiple values.
         * * `Result<T, Trap>` where `T` is another valid return type - interpreted as
         *   a function that returns `T` to wasm but is optionally allowed to also
         *   raise a trap.
         *
         * It's recommended, if possible, to use this function over the `Func`
         * constructor since this is generally easier to work with and also enables
         * a faster path for WebAssembly to call this function.
         */
        template <typename F,
                std::enable_if_t<WasmHostFunc<F>::Params::valid, bool> = true,
                std::enable_if_t<WasmHostFunc<F>::Results::valid, bool> = true>
        static Func wrap(Store::Context cx, F f) {
            using HostFunc = WasmHostFunc<F>;
            auto params = HostFunc::Params::types();
            auto results = HostFunc::Results::types();
            auto ty = FuncType::from_iters(params, results);
            wasmtime_func_t func;
            wasmtime_func_new_unchecked(cx.ptr, ty.ptr.get(), raw_callback_unchecked<F>,
                                        std::make_unique<F>(f).release(),
                                        raw_finalize<F>, &func);
            return func;
        }

        /**
         * \brief Invoke a WebAssembly function.
         *
         * This function will execute this WebAssembly function. This function muts be
         * defined within the `cx`'s store provided. The `params` argument is the list
         * of parameters that are passed to the wasm function, and the types of the
         * values within `params` must match the type signature of this function.
         *
         * This may return one of three values:
         *
         * * First the function could succeed, returning a vector of values
         *   representing the results of the function.
         * * Otherwise a `Trap` might be generated by the WebAssembly function.
         * * Finally an `Error` could be returned indicating that `params` were not of
         *   the right type.
         *
         * > **Note**: for optimized calls into WebAssembly where the function
         * > signature is statically known it's recommended to use `Func::typed` and
         * > `TypedFunc::call`.
         */
        [[nodiscard]] TrapResult<std::vector<Val>>
        call(Store::Context cx, const std::vector<Val> &params) const {
            std::vector<wasmtime_val_t> raw_params;
            raw_params.reserve(params.size());
            for (const auto &param : params) {
                raw_params.push_back(param.val);
            }
            size_t nresults = this->type(cx)->results().size();
            std::vector<wasmtime_val_t> raw_results(nresults);

            wasm_trap_t *trap = nullptr;
            auto *error =
                    wasmtime_func_call(cx.ptr, &func, raw_params.data(), raw_params.size(),
                                       raw_results.data(), raw_results.capacity(), &trap);
            if (error != nullptr) {
                return TrapError(Error(error));
            }
            if (trap != nullptr) {
                return TrapError(Trap(trap));
            }

            std::vector<Val> results;
            results.reserve(nresults);
            for (size_t i = 0; i < nresults; i++) {
                results.push_back(raw_results[i]);
            }
            return results;
        }

        /// Returns the type of this function.
        FuncType type(Store::Context cx) const {
            return wasmtime_func_type(cx.ptr, &func);
        }

        /**
         * \brief Statically checks this function against the provided types.
         *
         * This function will check whether it takes the statically known `Params`
         * and returns the statically known `Results`. If the type check succeeds then
         * a `TypedFunc` is returned which enables a faster method of invoking
         * WebAssembly functions.
         *
         * The `Params` and `Results` specified as template parameters here are the
         * parameters and results of the wasm function. They can either be a bare
         * type which means that the wasm takes/returns one value, or they can be a
         * `std::tuple<T...>` of types to represent multiple arguments or multiple
         * returns.
         *
         * The valid types for this function are those mentioned as the arguments
         * for `Func::wrap`.
         */
        template <typename Params, typename Results,
                std::enable_if_t<WasmTypeList<Params>::valid, bool> = true,
                std::enable_if_t<WasmTypeList<Results>::valid, bool> = true>
        Result<TypedFunc<Params, Results>> typed(Store::Context cx) const {
            auto ty = this->type(cx);
            if (!WasmTypeList<Params>::matches(ty->params()) ||
                !WasmTypeList<Results>::matches(ty->results())) {
                return Error("static type for this function does not match actual type");
            }
            TypedFunc<Params, Results> ret(*this);
            return ret;
        }

        /// Returns the raw underlying C API function this is using.
        const wasmtime_func_t &raw_func() const { return func; }
    };

/**
 * \brief A version of a WebAssembly `Func` where the type signature of the
 * function is statically known.
 */
    template <typename Params, typename Results> class TypedFunc {
        friend class Func;
        Func f;
        TypedFunc(Func func) : f(func) {}

    public:
        /**
         * \brief Calls this function with the provided parameters.
         *
         * This function is akin to `Func::call` except that since static type
         * information is available it statically takes its parameters and statically
         * returns its results.
         *
         * Note that this function still may return a `Trap` indicating that calling
         * the WebAssembly function failed.
         */
        Result<Results, Trap> call(Store::Context cx, Params params) const {
            std::array<wasmtime_val_raw_t, std::max(WasmTypeList<Params>::size,
                                                    WasmTypeList<Results>::size)>
                    storage;
            WasmTypeList<Params>::store(cx, storage.data(), params);
            auto *trap =
                    wasmtime_func_call_unchecked(cx.raw_context(), &f.func, storage.data());
            if (trap != nullptr) {
                return Trap(trap);
            }
            return WasmTypeList<Results>::load(cx, storage.data());
        }

        /// Returns the underlying un-typed `Func` for this function.
        const Func &func() const { return f; }
    };

    inline Val::Val(std::optional<Func> func) : val{} {
        val.kind = WASMTIME_FUNCREF;
        val.of.funcref.store_id = 0;
        val.of.funcref.index = 0;
        if (func) {
            val.of.funcref = (*func).func;
        }
    }

    inline Val::Val(Func func) : Val(std::optional(func)) {}
    inline Val::Val(ExternRef ptr) : Val(std::optional(ptr)) {}

    inline std::optional<Func> Val::funcref() const {
        if (val.kind != WASMTIME_FUNCREF) {
            std::abort();
        }
        if (val.of.funcref.store_id == 0) {
            return std::nullopt;
        }
        return Func(val.of.funcref);
    }

/// Definition for the `funcref` native wasm type
    template <> struct detail::WasmType<std::optional<Func>> {
        /// @private
        static const bool valid = true;
        /// @private
        static const ValKind kind = ValKind::FuncRef;
        /// @private
        static void store(Store::Context cx, wasmtime_val_raw_t *p,
                          const std::optional<Func> func) {
            if (func) {
                p->funcref = wasmtime_func_to_raw(cx.raw_context(), &func->raw_func());
            } else {
                p->funcref = 0;
            }
        }
        /// @private
        static std::optional<Func> load(Store::Context cx, wasmtime_val_raw_t *p) {
            if (p->funcref == 0) {
                return std::nullopt;
            }
            wasmtime_func_t ret;
            wasmtime_func_from_raw(cx.raw_context(), p->funcref, &ret);
            return ret;
        }
    };

/**
 * \brief A WebAssembly global.
 *
 * This class represents a WebAssembly global, either created through
 * instantiating a module or a host global. Globals contain a WebAssembly value
 * and can be read and optionally written to.
 *
 * Note that this type does not itself own any resources. It points to resources
 * owned within a `Store` and the `Store` must be passed in as the first
 * argument to the functions defined on `Global`. Note that if the wrong `Store`
 * is passed in then the process will be aborted.
 */
    class Global {
        friend class Instance;
        wasmtime_global_t global;

    public:
        /// Creates as global from the raw underlying C API representation.
        Global(wasmtime_global_t global) : global(global) {}

        /**
         * \brief Create a new WebAssembly global.
         *
         * \param cx the store in which to create the global
         * \param ty the type that this global will have
         * \param init the initial value of the global
         *
         * This function can fail if `init` does not have a value that matches `ty`.
         */
        [[nodiscard]] static Result<Global>
        create(Store::Context cx, const GlobalType &ty, const Val &init) {
            wasmtime_global_t global;
            auto *error = wasmtime_global_new(cx.ptr, ty.ptr.get(), &init.val, &global);
            if (error != nullptr) {
                return Error(error);
            }
            return Global(global);
        }

        /// Returns the type of this global.
        GlobalType type(Store::Context cx) const {
            return wasmtime_global_type(cx.ptr, &global);
        }

        /// Returns the current value of this global.
        Val get(Store::Context cx) const;

        /// Sets this global to a new value.
        ///
        /// This can fail if `val` has the wrong type or if this global isn't mutable.
        [[nodiscard]] Result<std::monostate> set(Store::Context cx,
                                                 const Val &val) const {
            auto *error = wasmtime_global_set(cx.ptr, &global, &val.val);
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }
    };

/**
 * \brief A WebAssembly table.
 *
 * This class represents a WebAssembly table, either created through
 * instantiating a module or a host table. Tables are contiguous vectors of
 * WebAssembly reference types, currently either `externref` or `funcref`.
 *
 * Note that this type does not itself own any resources. It points to resources
 * owned within a `Store` and the `Store` must be passed in as the first
 * argument to the functions defined on `Table`. Note that if the wrong `Store`
 * is passed in then the process will be aborted.
 */
    class Table {
        friend class Instance;
        wasmtime_table_t table;

    public:
        /// Creates a new table from the raw underlying C API representation.
        Table(wasmtime_table_t table) : table(table) {}

        /**
         * \brief Creates a new host-defined table.
         *
         * \param cx the store in which to create the table.
         * \param ty the type of the table to be created
         * \param init the initial value for all table slots.
         *
         * Returns an error if `init` has the wrong value for the `ty` specified.
         */
        [[nodiscard]] static Result<Table>
        create(Store::Context cx, const TableType &ty, const Val &init) {
            wasmtime_table_t table;
            auto *error = wasmtime_table_new(cx.ptr, ty.ptr.get(), &init.val, &table);
            if (error != nullptr) {
                return Error(error);
            }
            return Table(table);
        }

        /// Returns the type of this table.
        TableType type(Store::Context cx) const {
            return wasmtime_table_type(cx.ptr, &table);
        }

        /// Returns the size, in elements, that the table currently has.
        size_t size(Store::Context cx) const {
            return wasmtime_table_size(cx.ptr, &table);
        }

        /// Loads a value from the specified index in this table.
        ///
        /// Returns `std::nullopt` if `idx` is out of bounds.
        std::optional<Val> get(Store::Context cx, uint32_t idx) const {
            Val val;
            if (wasmtime_table_get(cx.ptr, &table, idx, &val.val)) {
                return val;
            }
            return std::nullopt;
        }

        /// Stores a value into the specified index in this table.
        ///
        /// Returns an error if `idx` is out of bounds or if `val` has the wrong type.
        [[nodiscard]] Result<std::monostate> set(Store::Context cx, uint32_t idx,
                                                 const Val &val) const {
            auto *error = wasmtime_table_set(cx.ptr, &table, idx, &val.val);
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /// Grow this table.
        ///
        /// \param cx the store that owns this table.
        /// \param delta the number of new elements to be added to this table.
        /// \param init the initial value of all new elements in this table.
        ///
        /// Returns an error if `init` has the wrong type for this table. Otherwise
        /// returns the previous size of the table before growth.
        [[nodiscard]] Result<uint32_t> grow(Store::Context cx, uint32_t delta,
                                            const Val &init) const {
            uint32_t prev = 0;
            auto *error = wasmtime_table_grow(cx.ptr, &table, delta, &init.val, &prev);
            if (error != nullptr) {
                return Error(error);
            }
            return prev;
        }
    };

// gcc 8.3.0 seems to require that this comes after the definition of `Table`. I
// don't know why...
    inline Val Global::get(Store::Context cx) const {
        Val val;
        wasmtime_global_get(cx.ptr, &global, &val.val);
        return val;
    }

/**
 * \brief A WebAssembly linear memory.
 *
 * This class represents a WebAssembly memory, either created through
 * instantiating a module or a host memory.
 *
 * Note that this type does not itself own any resources. It points to resources
 * owned within a `Store` and the `Store` must be passed in as the first
 * argument to the functions defined on `Table`. Note that if the wrong `Store`
 * is passed in then the process will be aborted.
 */
    class Memory {
        friend class Instance;
        wasmtime_memory_t memory;

    public:
        /// Creates a new memory from the raw underlying C API representation.
        Memory(wasmtime_memory_t memory) : memory(memory) {}

        /// Creates a new host-defined memory with the type specified.
        [[nodiscard]] static Result<Memory> create(Store::Context cx,
                                                   const MemoryType &ty) {
            wasmtime_memory_t memory;
            auto *error = wasmtime_memory_new(cx.ptr, ty.ptr.get(), &memory);
            if (error != nullptr) {
                return Error(error);
            }
            return Memory(memory);
        }

        /// Returns the type of this memory.
        MemoryType type(Store::Context cx) const {
            return wasmtime_memory_type(cx.ptr, &memory);
        }

        /// Returns the size, in WebAssembly pages, of this memory.
        uint64_t size(Store::Context cx) const {
            return wasmtime_memory_size(cx.ptr, &memory);
        }

        /// Returns a `span` of where this memory is located in the host.
        ///
        /// Note that embedders need to be very careful in their usage of the returned
        /// `span`. It can be invalidated with calls to `grow` and/or calls into
        /// WebAssembly.
        Span<uint8_t> data(Store::Context cx) const {
            auto *base = wasmtime_memory_data(cx.ptr, &memory);
            auto size = wasmtime_memory_data_size(cx.ptr, &memory);
            return {base, size};
        }

        /// Grows the memory by `delta` WebAssembly pages.
        ///
        /// On success returns the previous size of this memory in units of
        /// WebAssembly pages.
        [[nodiscard]] Result<uint64_t> grow(Store::Context cx, uint64_t delta) const {
            uint64_t prev = 0;
            auto *error = wasmtime_memory_grow(cx.ptr, &memory, delta, &prev);
            if (error != nullptr) {
                return Error(error);
            }
            return prev;
        }
    };

/**
 * \brief A WebAssembly instance.
 *
 * This class represents a WebAssembly instance, created by instantiating a
 * module. An instance is the collection of items exported by the module, which
 * can be accessed through the `Store` that owns the instance.
 *
 * Note that this type does not itself own any resources. It points to resources
 * owned within a `Store` and the `Store` must be passed in as the first
 * argument to the functions defined on `Instance`. Note that if the wrong
 * `Store` is passed in then the process will be aborted.
 */
    class Instance {
        friend class Linker;
        friend class Caller;

        wasmtime_instance_t instance;

        static Extern cvt(wasmtime_extern_t &e) {
            switch (e.kind) {
                case WASMTIME_EXTERN_FUNC:
                    return Func(e.of.func);
                case WASMTIME_EXTERN_GLOBAL:
                    return Global(e.of.global);
                case WASMTIME_EXTERN_MEMORY:
                    return Memory(e.of.memory);
                case WASMTIME_EXTERN_TABLE:
                    return Table(e.of.table);
            }
            std::abort();
        }

        static void cvt(const Extern &e, wasmtime_extern_t &raw) {
            if (const auto *func = std::get_if<Func>(&e)) {
                raw.kind = WASMTIME_EXTERN_FUNC;
                raw.of.func = func->func;
            } else if (const auto *global = std::get_if<Global>(&e)) {
                raw.kind = WASMTIME_EXTERN_GLOBAL;
                raw.of.global = global->global;
            } else if (const auto *table = std::get_if<Table>(&e)) {
                raw.kind = WASMTIME_EXTERN_TABLE;
                raw.of.table = table->table;
            } else if (const auto *memory = std::get_if<Memory>(&e)) {
                raw.kind = WASMTIME_EXTERN_MEMORY;
                raw.of.memory = memory->memory;
            } else {
                std::abort();
            }
        }

    public:
        /// Creates a new instance from the raw underlying C API representation.
        Instance(wasmtime_instance_t instance) : instance(instance) {}

        /**
         * \brief Instantiates the module `m` with the provided `imports`
         *
         * \param cx the store in which to instantiate the provided module
         * \param m the module to instantiate
         * \param imports the list of imports to use to instantiate the module
         *
         * This `imports` parameter is expected to line up 1:1 with the imports
         * required by the `m`. The type of `m` can be inspected to determine in which
         * order to provide the imports. Note that this is a relatively low-level API
         * and it's generally recommended to use `Linker` instead for name-based
         * instantiation.
         *
         * This function can return an error if any of the `imports` have the wrong
         * type, or if the wrong number of `imports` is provided.
         */
        [[nodiscard]] static TrapResult<Instance>
        create(Store::Context cx, const Module &m,
               const std::vector<Extern> &imports) {
            std::vector<wasmtime_extern_t> raw_imports;
            for (const auto &item : imports) {
                raw_imports.push_back(wasmtime_extern_t{});
                auto &last = raw_imports.back();
                Instance::cvt(item, last);
            }
            wasmtime_instance_t instance;
            wasm_trap_t *trap = nullptr;
            auto *error = wasmtime_instance_new(cx.ptr, m.ptr.get(), raw_imports.data(),
                                                raw_imports.size(), &instance, &trap);
            if (error != nullptr) {
                return TrapError(Error(error));
            }
            if (trap != nullptr) {
                return TrapError(Trap(trap));
            }
            return Instance(instance);
        }

        /**
         * \brief Load an instance's export by name.
         *
         * This function will look for an export named `name` on this instance and, if
         * found, return it as an `Extern`.
         */
        std::optional<Extern> get(Store::Context cx, std::string_view name) {
            wasmtime_extern_t e;
            if (!wasmtime_instance_export_get(cx.ptr, &instance, name.data(),
                                              name.size(), &e)) {
                return std::nullopt;
            }
            return Instance::cvt(e);
        }

        /**
         * \brief Load an instance's export by index.
         *
         * This function will look for the `idx`th export of this instance. This will
         * return both the name of the export as well as the exported item itself.
         */
        std::optional<std::pair<std::string_view, Extern>> get(Store::Context cx,
                                                               size_t idx) {
            wasmtime_extern_t e;
            // I'm not sure why clang-tidy thinks this is using va_list or anything
            // related to that...
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
            char *name = nullptr;
            size_t len = 0;
            if (!wasmtime_instance_export_nth(cx.ptr, &instance, idx, &name, &len,
                                              &e)) {
                return std::nullopt;
            }
            std::string_view n(name, len);
            return std::pair(n, Instance::cvt(e));
        }
    };

    inline std::optional<Extern> Caller::get_export(std::string_view name) {
        wasmtime_extern_t item;
        if (wasmtime_caller_export_get(ptr, name.data(), name.size(), &item)) {
            return Instance::cvt(item);
        }
        return std::nullopt;
    }

/**
 * \brief Helper class for linking modules together with name-based resolution.
 *
 * This class is used for easily instantiating `Module`s by defining names into
 * the linker and performing name-based resolution during instantiation. A
 * `Linker` can also be used to link in WASI functions to instantiate a module.
 */
    class Linker {
        struct deleter {
            void operator()(wasmtime_linker_t *p) const { wasmtime_linker_delete(p); }
        };

        std::unique_ptr<wasmtime_linker_t, deleter> ptr;

    public:
        /// Creates a new linker which will instantiate in the given engine.
        explicit Linker(Engine &engine)
                : ptr(wasmtime_linker_new(engine.ptr.get())) {}

        /// Configures whether shadowing previous names is allowed or not.
        ///
        /// By default shadowing is not allowed.
        void allow_shadowing(bool allow) {
            wasmtime_linker_allow_shadowing(ptr.get(), allow);
        }

        /// Defines the provided item into this linker with the given name.
        [[nodiscard]] Result<std::monostate>
        define(std::string_view module, std::string_view name, const Extern &item) {
            wasmtime_extern_t raw;
            Instance::cvt(item, raw);
            auto *error =
                    wasmtime_linker_define(ptr.get(), module.data(), module.size(),
                                           name.data(), name.size(), &raw);
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /// Defines WASI functions within this linker.
        ///
        /// Note that `Store::Context::set_wasi` must also be used for instantiated
        /// modules to have access to configured WASI state.
        [[nodiscard]] Result<std::monostate> define_wasi() {
            auto *error = wasmtime_linker_define_wasi(ptr.get());
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /// Defines all exports of the `instance` provided in this linker with the
        /// given module name of `name`.
        [[nodiscard]] Result<std::monostate>
        define_instance(Store::Context cx, std::string_view name, Instance instance) {
            auto *error = wasmtime_linker_define_instance(
                    ptr.get(), cx.ptr, name.data(), name.size(), &instance.instance);
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /// Instantiates the module `m` provided within the store `cx` using the items
        /// defined within this linker.
        [[nodiscard]] TrapResult<Instance> instantiate(Store::Context cx,
                                                       const Module &m) {
            wasmtime_instance_t instance;
            wasm_trap_t *trap = nullptr;
            auto *error = wasmtime_linker_instantiate(ptr.get(), cx.ptr, m.ptr.get(),
                                                      &instance, &trap);
            if (error != nullptr) {
                return TrapError(Error(error));
            }
            if (trap != nullptr) {
                return TrapError(Trap(trap));
            }
            return Instance(instance);
        }

        /// Defines instantiations of the module `m` within this linker under the
        /// given `name`.
        [[nodiscard]] Result<std::monostate>
        module(Store::Context cx, std::string_view name, const Module &m) {
            auto *error = wasmtime_linker_module(ptr.get(), cx.ptr, name.data(),
                                                 name.size(), m.ptr.get());
            if (error != nullptr) {
                return Error(error);
            }
            return std::monostate();
        }

        /// Attempts to load the specified named item from this linker, returning
        /// `std::nullopt` if it was not defiend.
        [[nodiscard]] std::optional<Extern>
        get(Store::Context cx, std::string_view module, std::string_view name) {
            wasmtime_extern_t item;
            if (wasmtime_linker_get(ptr.get(), cx.ptr, module.data(), module.size(),
                                    name.data(), name.size(), &item)) {
                return Instance::cvt(item);
            }
            return std::nullopt;
        }

        /// Defines a new function in this linker in the style of the `Func`
        /// constructor.
        template <typename F,
                std::enable_if_t<
                        std::is_invocable_r_v<Result<std::monostate, Trap>, F, Caller,
                                Span<const Val>, Span<Val>>,
                        bool> = true>
        [[nodiscard]] Result<std::monostate> func_new(std::string_view module,
                                                      std::string_view name,
                                                      const FuncType &ty, F f) {

            auto *error = wasmtime_linker_define_func(
                    ptr.get(), module.data(), module.length(), name.data(), name.length(),
                    ty.ptr.get(), Func::raw_callback<F>, std::make_unique<F>(f).release(),
                    Func::raw_finalize<F>);

            if (error != nullptr) {
                return Error(error);
            }

            return std::monostate();
        }

        /// Defines a new function in this linker in the style of the `Func::wrap`
        /// constructor.
        template <typename F,
                std::enable_if_t<WasmHostFunc<F>::Params::valid, bool> = true,
                std::enable_if_t<WasmHostFunc<F>::Results::valid, bool> = true>
        [[nodiscard]] Result<std::monostate> func_wrap(std::string_view module,
                                                       std::string_view name, F f) {
            using HostFunc = WasmHostFunc<F>;
            auto params = HostFunc::Params::types();
            auto results = HostFunc::Results::types();
            auto ty = FuncType::from_iters(params, results);
            auto *error = wasmtime_linker_define_func_unchecked(
                    ptr.get(), module.data(), module.length(), name.data(), name.length(),
                    ty.ptr.get(), Func::raw_callback_unchecked<F>,
                    std::make_unique<F>(f).release(), Func::raw_finalize<F>);

            if (error != nullptr) {
                return Error(error);
            }

            return std::monostate();
        }

        /// Loads the "default" function, according to WASI commands and reactors, of
        /// the module named `name` in this linker.
        [[nodiscard]] Result<Func> get_default(Store::Context cx,
                                               std::string_view name) {
            wasmtime_func_t item;
            auto *error = wasmtime_linker_get_default(ptr.get(), cx.ptr, name.data(),
                                                      name.size(), &item);
            if (error != nullptr) {
                return Error(error);
            }
            return Func(item);
        }
    };

} // namespace wasmtime

#endif // WASMTIME_HH