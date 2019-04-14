//
//  Bismillah ar-Rahmaan ar-Raheem
//
//  Easylogging++ v9.96.7
//  Single-header only, cross-platform logging library for C++ applications
//
//  Copyright (c) 2012-2018 Zuhd Web Services
//  Copyright (c) 2012-2018 @abumusamq
//
//  This library is released under the MIT Licence.
//  https://github.com/zuhd-org/easyloggingpp/blob/master/LICENSE
//
//  https://zuhd.org
//  http://muflihun.com
//

#ifndef EASYLOGGINGPP_H
#define EASYLOGGINGPP_H
// Compilers and C++0x/C++11 Evaluation
#if __cplusplus >= 201103L
#  define ELPP_CXX11 1
#endif  // __cplusplus >= 201103L
#if (defined(__GNUC__))
#  define ELPP_COMPILER_GCC 1
#else
#  define ELPP_COMPILER_GCC 0
#endif
#if ELPP_COMPILER_GCC
#    define ELPP_GCC_VERSION (__GNUC__ * 10000 \
+ __GNUC_MINOR__ * 100 \
+ __GNUC_PATCHLEVEL__)
#  if defined(__GXX_EXPERIMENTAL_CXX0X__)
#    define ELPP_CXX0X 1
#  endif
#endif
// Visual C++
#if defined(_MSC_VER)
#  define ELPP_COMPILER_MSVC 1
#else
#  define ELPP_COMPILER_MSVC 0
#endif
#define ELPP_CRT_DBG_WARNINGS ELPP_COMPILER_MSVC
#if ELPP_COMPILER_MSVC
#  if (_MSC_VER == 1600)
#    define ELPP_CXX0X 1
#  elif(_MSC_VER >= 1700)
#    define ELPP_CXX11 1
#  endif
#endif
// Clang++
#if (defined(__clang__) && (__clang__ == 1))
#  define ELPP_COMPILER_CLANG 1
#else
#  define ELPP_COMPILER_CLANG 0
#endif
#if ELPP_COMPILER_CLANG
#  if __has_include(<thread>)
#    include <cstddef> // Make __GLIBCXX__ defined when using libstdc++
#    if !defined(__GLIBCXX__) || __GLIBCXX__ >= 20150426
#      define ELPP_CLANG_SUPPORTS_THREAD
#    endif // !defined(__GLIBCXX__) || __GLIBCXX__ >= 20150426
#  endif // __has_include(<thread>)
#endif
#if (defined(__MINGW32__) || defined(__MINGW64__))
#  define ELPP_MINGW 1
#else
#  define ELPP_MINGW 0
#endif
#if (defined(__CYGWIN__) && (__CYGWIN__ == 1))
#  define ELPP_CYGWIN 1
#else
#  define ELPP_CYGWIN 0
#endif
#if (defined(__INTEL_COMPILER))
#  define ELPP_COMPILER_INTEL 1
#else
#  define ELPP_COMPILER_INTEL 0
#endif
// Operating System Evaluation
// Windows
#if (defined(_WIN32) || defined(_WIN64))
#  define ELPP_OS_WINDOWS 1
#else
#  define ELPP_OS_WINDOWS 0
#endif
// Linux
#if (defined(__linux) || defined(__linux__))
#  define ELPP_OS_LINUX 1
#else
#  define ELPP_OS_LINUX 0
#endif
#if (defined(__APPLE__))
#  define ELPP_OS_MAC 1
#else
#  define ELPP_OS_MAC 0
#endif
#if (defined(__FreeBSD__) || defined(__FreeBSD_kernel__))
#  define ELPP_OS_FREEBSD 1
#else
#  define ELPP_OS_FREEBSD 0
#endif
#if (defined(__sun))
#  define ELPP_OS_SOLARIS 1
#else
#  define ELPP_OS_SOLARIS 0
#endif
#if (defined(_AIX))
#  define ELPP_OS_AIX 1
#else
#  define ELPP_OS_AIX 0
#endif
#if (defined(__NetBSD__))
#  define ELPP_OS_NETBSD 1
#else
#  define ELPP_OS_NETBSD 0
#endif
#if defined(__EMSCRIPTEN__)
#  define ELPP_OS_EMSCRIPTEN 1
#else
#  define ELPP_OS_EMSCRIPTEN 0
#endif
// Unix
#if ((ELPP_OS_LINUX || ELPP_OS_MAC || ELPP_OS_FREEBSD || ELPP_OS_NETBSD || ELPP_OS_SOLARIS || ELPP_OS_AIX || ELPP_OS_EMSCRIPTEN) && (!ELPP_OS_WINDOWS))
#  define ELPP_OS_UNIX 1
#else
#  define ELPP_OS_UNIX 0
#endif
#if (defined(__ANDROID__))
#  define ELPP_OS_ANDROID 1
#else
#  define ELPP_OS_ANDROID 0
#endif
// Evaluating Cygwin as *nix OS
#if !ELPP_OS_UNIX && !ELPP_OS_WINDOWS && ELPP_CYGWIN
#  undef ELPP_OS_UNIX
#  undef ELPP_OS_LINUX
#  define ELPP_OS_UNIX 1
#  define ELPP_OS_LINUX 1
#endif //  !ELPP_OS_UNIX && !ELPP_OS_WINDOWS && ELPP_CYGWIN
#if !defined(ELPP_INTERNAL_DEBUGGING_OUT_INFO)
#  define ELPP_INTERNAL_DEBUGGING_OUT_INFO std::cout
#endif // !defined(ELPP_INTERNAL_DEBUGGING_OUT)
#if !defined(ELPP_INTERNAL_DEBUGGING_OUT_ERROR)
#  define ELPP_INTERNAL_DEBUGGING_OUT_ERROR std::cerr
#endif // !defined(ELPP_INTERNAL_DEBUGGING_OUT)
#if !defined(ELPP_INTERNAL_DEBUGGING_ENDL)
#  define ELPP_INTERNAL_DEBUGGING_ENDL std::endl
#endif // !defined(ELPP_INTERNAL_DEBUGGING_OUT)
#if !defined(ELPP_INTERNAL_DEBUGGING_MSG)
#  define ELPP_INTERNAL_DEBUGGING_MSG(msg) msg
#endif // !defined(ELPP_INTERNAL_DEBUGGING_OUT)
// Internal Assertions and errors
#if !defined(ELPP_DISABLE_ASSERT)
#  if (defined(ELPP_DEBUG_ASSERT_FAILURE))
#    define ELPP_ASSERT(expr, msg) if (!(expr)) { \
std::stringstream internalInfoStream; internalInfoStream << msg; \
ELPP_INTERNAL_DEBUGGING_OUT_ERROR \
<< "EASYLOGGING++ ASSERTION FAILED (LINE: " << __LINE__ << ") [" #expr << "] WITH MESSAGE \"" \
<< ELPP_INTERNAL_DEBUGGING_MSG(internalInfoStream.str()) << "\"" << ELPP_INTERNAL_DEBUGGING_ENDL; base::utils::abort(1, \
"ELPP Assertion failure, please define ELPP_DEBUG_ASSERT_FAILURE"); }
#  else
#    define ELPP_ASSERT(expr, msg) if (!(expr)) { \
std::stringstream internalInfoStream; internalInfoStream << msg; \
ELPP_INTERNAL_DEBUGGING_OUT_ERROR\
<< "ASSERTION FAILURE FROM EASYLOGGING++ (LINE: " \
<< __LINE__ << ") [" #expr << "] WITH MESSAGE \"" << ELPP_INTERNAL_DEBUGGING_MSG(internalInfoStream.str()) << "\"" \
<< ELPP_INTERNAL_DEBUGGING_ENDL; }
#  endif  // (defined(ELPP_DEBUG_ASSERT_FAILURE))
#else
#  define ELPP_ASSERT(x, y)
#endif  //(!defined(ELPP_DISABLE_ASSERT)
#if ELPP_COMPILER_MSVC
#  define ELPP_INTERNAL_DEBUGGING_WRITE_PERROR \
{ char buff[256]; strerror_s(buff, 256, errno); \
ELPP_INTERNAL_DEBUGGING_OUT_ERROR << ": " << buff << " [" << errno << "]";} (void)0
#else
#  define ELPP_INTERNAL_DEBUGGING_WRITE_PERROR \
ELPP_INTERNAL_DEBUGGING_OUT_ERROR << ": " << strerror(errno) << " [" << errno << "]"; (void)0
#endif  // ELPP_COMPILER_MSVC
#if defined(ELPP_DEBUG_ERRORS)
#  if !defined(ELPP_INTERNAL_ERROR)
#    define ELPP_INTERNAL_ERROR(msg, pe) { \
std::stringstream internalInfoStream; internalInfoStream << "<ERROR> " << msg; \
ELPP_INTERNAL_DEBUGGING_OUT_ERROR \
<< "ERROR FROM EASYLOGGING++ (LINE: " << __LINE__ << ") " \
<< ELPP_INTERNAL_DEBUGGING_MSG(internalInfoStream.str()) << ELPP_INTERNAL_DEBUGGING_ENDL; \
if (pe) { ELPP_INTERNAL_DEBUGGING_OUT_ERROR << "    "; ELPP_INTERNAL_DEBUGGING_WRITE_PERROR; }} (void)0
#  endif
#else
#  undef ELPP_INTERNAL_INFO
#  define ELPP_INTERNAL_ERROR(msg, pe)
#endif  // defined(ELPP_DEBUG_ERRORS)
#if (defined(ELPP_DEBUG_INFO))
#  if !(defined(ELPP_INTERNAL_INFO_LEVEL))
#    define ELPP_INTERNAL_INFO_LEVEL 9
#  endif  // !(defined(ELPP_INTERNAL_INFO_LEVEL))
#  if !defined(ELPP_INTERNAL_INFO)
#    define ELPP_INTERNAL_INFO(lvl, msg) { if (lvl <= ELPP_INTERNAL_INFO_LEVEL) { \
std::stringstream internalInfoStream; internalInfoStream << "<INFO> " << msg; \
ELPP_INTERNAL_DEBUGGING_OUT_INFO << ELPP_INTERNAL_DEBUGGING_MSG(internalInfoStream.str()) \
<< ELPP_INTERNAL_DEBUGGING_ENDL; }}
#  endif
#else
#  undef ELPP_INTERNAL_INFO
#  define ELPP_INTERNAL_INFO(lvl, msg)
#endif  // (defined(ELPP_DEBUG_INFO))
#if (defined(ELPP_FEATURE_ALL)) || (defined(ELPP_FEATURE_CRASH_LOG))
#  if (ELPP_COMPILER_GCC && !ELPP_MINGW && !ELPP_OS_ANDROID && !ELPP_OS_EMSCRIPTEN)
#    define ELPP_STACKTRACE 1
#  else
#      if ELPP_COMPILER_MSVC
#         pragma message("Stack trace not available for this compiler")
#      else
#         warning "Stack trace not available for this compiler";
#      endif  // ELPP_COMPILER_MSVC
#    define ELPP_STACKTRACE 0
#  endif  // ELPP_COMPILER_GCC
#else
#    define ELPP_STACKTRACE 0
#endif  // (defined(ELPP_FEATURE_ALL)) || (defined(ELPP_FEATURE_CRASH_LOG))
// Miscellaneous macros
#define ELPP_UNUSED(x) (void)x
#if ELPP_OS_UNIX
// Log file permissions for unix-based systems
#  define ELPP_LOG_PERMS S_IRUSR | S_IWUSR | S_IXUSR | S_IWGRP | S_IRGRP | S_IXGRP | S_IWOTH | S_IXOTH
#endif  // ELPP_OS_UNIX
#if defined(ELPP_AS_DLL) && ELPP_COMPILER_MSVC
#  if defined(ELPP_EXPORT_SYMBOLS)
#    define ELPP_EXPORT __declspec(dllexport)
#  else
#    define ELPP_EXPORT __declspec(dllimport)
#  endif  // defined(ELPP_EXPORT_SYMBOLS)
#else
#  define ELPP_EXPORT
#endif  // defined(ELPP_AS_DLL) && ELPP_COMPILER_MSVC
// Some special functions that are VC++ specific
#undef STRTOK
#undef STRERROR
#undef STRCAT
#undef STRCPY
#if ELPP_CRT_DBG_WARNINGS
#  define STRTOK(a, b, c) strtok_s(a, b, c)
#  define STRERROR(a, b, c) strerror_s(a, b, c)
#  define STRCAT(a, b, len) strcat_s(a, len, b)
#  define STRCPY(a, b, len) strcpy_s(a, len, b)
#else
#  define STRTOK(a, b, c) strtok(a, b)
#  define STRERROR(a, b, c) strerror(c)
#  define STRCAT(a, b, len) strcat(a, b)
#  define STRCPY(a, b, len) strcpy(a, b)
#endif
// Compiler specific support evaluations
#if (ELPP_MINGW && !defined(ELPP_FORCE_USE_STD_THREAD))
#  define ELPP_USE_STD_THREADING 0
#else
#  if ((ELPP_COMPILER_CLANG && defined(ELPP_CLANG_SUPPORTS_THREAD)) || \
       (!ELPP_COMPILER_CLANG && defined(ELPP_CXX11)) || \
       defined(ELPP_FORCE_USE_STD_THREAD))
#    define ELPP_USE_STD_THREADING 1
#  else
#    define ELPP_USE_STD_THREADING 0
#  endif
#endif
#undef ELPP_FINAL
#if ELPP_COMPILER_INTEL || (ELPP_GCC_VERSION < 40702)
#  define ELPP_FINAL
#else
#  define ELPP_FINAL final
#endif  // ELPP_COMPILER_INTEL || (ELPP_GCC_VERSION < 40702)
#if defined(ELPP_EXPERIMENTAL_ASYNC)
#  define ELPP_ASYNC_LOGGING 1
#else
#  define ELPP_ASYNC_LOGGING 0
#endif // defined(ELPP_EXPERIMENTAL_ASYNC)
#if defined(ELPP_THREAD_SAFE) || ELPP_ASYNC_LOGGING
#  define ELPP_THREADING_ENABLED 1
#else
#  define ELPP_THREADING_ENABLED 0
#endif  // defined(ELPP_THREAD_SAFE) || ELPP_ASYNC_LOGGING
// Function macro ELPP_FUNC
#undef ELPP_FUNC
#if ELPP_COMPILER_MSVC  // Visual C++
#  define ELPP_FUNC __FUNCSIG__
#elif ELPP_COMPILER_GCC  // GCC
#  define ELPP_FUNC __PRETTY_FUNCTION__
#elif ELPP_COMPILER_INTEL  // Intel C++
#  define ELPP_FUNC __PRETTY_FUNCTION__
#elif ELPP_COMPILER_CLANG  // Clang++
#  define ELPP_FUNC __PRETTY_FUNCTION__
#else
#  if defined(__func__)
#    define ELPP_FUNC __func__
#  else
#    define ELPP_FUNC ""
#  endif  // defined(__func__)
#endif  // defined(_MSC_VER)
#undef ELPP_VARIADIC_TEMPLATES_SUPPORTED
// Keep following line commented until features are fixed
#define ELPP_VARIADIC_TEMPLATES_SUPPORTED \
(ELPP_COMPILER_GCC || ELPP_COMPILER_CLANG || ELPP_COMPILER_INTEL || (ELPP_COMPILER_MSVC && _MSC_VER >= 1800))
// Logging Enable/Disable macros
#if defined(ELPP_DISABLE_LOGS)
#define ELPP_LOGGING_ENABLED 0
#else
#define ELPP_LOGGING_ENABLED 1
#endif
#if (!defined(ELPP_DISABLE_DEBUG_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_DEBUG_LOG 1
#else
#  define ELPP_DEBUG_LOG 0
#endif  // (!defined(ELPP_DISABLE_DEBUG_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!defined(ELPP_DISABLE_INFO_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_INFO_LOG 1
#else
#  define ELPP_INFO_LOG 0
#endif  // (!defined(ELPP_DISABLE_INFO_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!defined(ELPP_DISABLE_WARNING_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_WARNING_LOG 1
#else
#  define ELPP_WARNING_LOG 0
#endif  // (!defined(ELPP_DISABLE_WARNING_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!defined(ELPP_DISABLE_ERROR_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_ERROR_LOG 1
#else
#  define ELPP_ERROR_LOG 0
#endif  // (!defined(ELPP_DISABLE_ERROR_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!defined(ELPP_DISABLE_FATAL_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_FATAL_LOG 1
#else
#  define ELPP_FATAL_LOG 0
#endif  // (!defined(ELPP_DISABLE_FATAL_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!defined(ELPP_DISABLE_TRACE_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_TRACE_LOG 1
#else
#  define ELPP_TRACE_LOG 0
#endif  // (!defined(ELPP_DISABLE_TRACE_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!defined(ELPP_DISABLE_VERBOSE_LOGS) && (ELPP_LOGGING_ENABLED))
#  define ELPP_VERBOSE_LOG 1
#else
#  define ELPP_VERBOSE_LOG 0
#endif  // (!defined(ELPP_DISABLE_VERBOSE_LOGS) && (ELPP_LOGGING_ENABLED))
#if (!(ELPP_CXX0X || ELPP_CXX11))
#   error "C++0x (or higher) support not detected! (Is `-std=c++11' missing?)"
#endif  // (!(ELPP_CXX0X || ELPP_CXX11))
// Headers
#if defined(ELPP_SYSLOG)
#   include <syslog.h>
#endif  // defined(ELPP_SYSLOG)
#include <ctime>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <cwchar>
#include <csignal>
#include <cerrno>
#include <cstdarg>
#if defined(ELPP_UNICODE)
#   include <locale>
#  if ELPP_OS_WINDOWS
#      include <codecvt>
#  endif // ELPP_OS_WINDOWS
#endif  // defined(ELPP_UNICODE)
#if ELPP_STACKTRACE
#   include <cxxabi.h>
#   include <execinfo.h>
#endif  // ELPP_STACKTRACE
#if ELPP_OS_ANDROID
#   include <sys/system_properties.h>
#endif  // ELPP_OS_ANDROID
#if ELPP_OS_UNIX
#   include <sys/stat.h>
#   include <sys/time.h>
#elif ELPP_OS_WINDOWS
#   include <direct.h>
#   include <windows.h>
#  if defined(WIN32_LEAN_AND_MEAN)
#      if defined(ELPP_WINSOCK2)
#         include <winsock2.h>
#      else
#         include <winsock.h>
#      endif // defined(ELPP_WINSOCK2)
#  endif // defined(WIN32_LEAN_AND_MEAN)
#endif  // ELPP_OS_UNIX
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <utility>
#include <functional>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <type_traits>
#if ELPP_THREADING_ENABLED
#  if ELPP_USE_STD_THREADING
#      include <mutex>
#      include <thread>
#  else
#      if ELPP_OS_UNIX
#         include <pthread.h>
#      endif  // ELPP_OS_UNIX
#  endif  // ELPP_USE_STD_THREADING
#endif  // ELPP_THREADING_ENABLED
#if ELPP_ASYNC_LOGGING
#  if defined(ELPP_NO_SLEEP_FOR)
#      include <unistd.h>
#  endif  // defined(ELPP_NO_SLEEP_FOR)
#   include <thread>
#   include <queue>
#   include <condition_variable>
#endif  // ELPP_ASYNC_LOGGING
#if defined(ELPP_STL_LOGGING)
// For logging STL based templates
#   include <list>
#   include <queue>
#   include <deque>
#   include <set>
#   include <bitset>
#   include <stack>
#  if defined(ELPP_LOG_STD_ARRAY)
#      include <array>
#  endif  // defined(ELPP_LOG_STD_ARRAY)
#  if defined(ELPP_LOG_UNORDERED_SET)
#      include <unordered_set>
#  endif  // defined(ELPP_UNORDERED_SET)
#endif  // defined(ELPP_STL_LOGGING)
#if defined(ELPP_QT_LOGGING)
// For logging Qt based classes & templates
#   include <QString>
#   include <QByteArray>
#   include <QVector>
#   include <QList>
#   include <QPair>
#   include <QMap>
#   include <QQueue>
#   include <QSet>
#   include <QLinkedList>
#   include <QHash>
#   include <QMultiHash>
#   include <QStack>
#endif  // defined(ELPP_QT_LOGGING)
#if defined(ELPP_BOOST_LOGGING)
// For logging boost based classes & templates
#   include <boost/container/vector.hpp>
#   include <boost/container/stable_vector.hpp>
#   include <boost/container/list.hpp>
#   include <boost/container/deque.hpp>
#   include <boost/container/map.hpp>
#   include <boost/container/flat_map.hpp>
#   include <boost/container/set.hpp>
#   include <boost/container/flat_set.hpp>
#endif  // defined(ELPP_BOOST_LOGGING)
#if defined(ELPP_WXWIDGETS_LOGGING)
// For logging wxWidgets based classes & templates
#   include <wx/vector.h>
#endif  // defined(ELPP_WXWIDGETS_LOGGING)
#if defined(ELPP_UTC_DATETIME)
#   define elpptime_r gmtime_r
#   define elpptime_s gmtime_s
#   define elpptime   gmtime
#else
#   define elpptime_r localtime_r
#   define elpptime_s localtime_s
#   define elpptime   localtime
#endif  // defined(ELPP_UTC_DATETIME)
// Forward declarations
namespace el {
class Logger;
class LogMessage;
class PerformanceTrackingData;
class Loggers;
class Helpers;
template <typename T> class Callback;
class LogDispatchCallback;
class PerformanceTrackingCallback;
class LoggerRegistrationCallback;
class LogDispatchData;
namespace base {
class Storage;
class RegisteredLoggers;
class PerformanceTracker;
class MessageBuilder;
class Writer;
class PErrorWriter;
class LogDispatcher;
class DefaultLogBuilder;
class DefaultLogDispatchCallback;
#if ELPP_ASYNC_LOGGING
class AsyncLogDispatchCallback;
class AsyncDispatchWorker;
#endif // ELPP_ASYNC_LOGGING
class DefaultPerformanceTrackingCallback;
}  // namespace base
}  // namespace el
/// @brief Easylogging++ entry namespace
namespace el {
/// @brief Namespace containing base/internal functionality used by Easylogging++
namespace base {
/// @brief Data types used by Easylogging++
namespace type {
#undef ELPP_LITERAL
#undef ELPP_STRLEN
#undef ELPP_COUT
#if defined(ELPP_UNICODE)
#  define ELPP_LITERAL(txt) L##txt
#  define ELPP_STRLEN wcslen
#  if defined ELPP_CUSTOM_COUT
#    define ELPP_COUT ELPP_CUSTOM_COUT
#  else
#    define ELPP_COUT std::wcout
#  endif  // defined ELPP_CUSTOM_COUT
typedef wchar_t char_t;
typedef std::wstring string_t;
typedef std::wstringstream stringstream_t;
typedef std::wfstream fstream_t;
typedef std::wostream ostream_t;
#else
#  define ELPP_LITERAL(txt) txt
#  define ELPP_STRLEN strlen
#  if defined ELPP_CUSTOM_COUT
#    define ELPP_COUT ELPP_CUSTOM_COUT
#  else
#    define ELPP_COUT std::cout
#  endif  // defined ELPP_CUSTOM_COUT
typedef char char_t;
typedef std::string string_t;
typedef std::stringstream stringstream_t;
typedef std::fstream fstream_t;
typedef std::ostream ostream_t;
#endif  // defined(ELPP_UNICODE)
#if defined(ELPP_CUSTOM_COUT_LINE)
#  define ELPP_COUT_LINE(logLine) ELPP_CUSTOM_COUT_LINE(logLine)
#else
#  define ELPP_COUT_LINE(logLine) logLine << std::flush
#endif // defined(ELPP_CUSTOM_COUT_LINE)
typedef unsigned int EnumType;
typedef unsigned short VerboseLevel;
typedef unsigned long int LineNumber;
typedef std::shared_ptr<base::Storage> StoragePointer;
typedef std::shared_ptr<LogDispatchCallback> LogDispatchCallbackPtr;
typedef std::shared_ptr<PerformanceTrackingCallback> PerformanceTrackingCallbackPtr;
typedef std::shared_ptr<LoggerRegistrationCallback> LoggerRegistrationCallbackPtr;
typedef std::unique_ptr<el::base::PerformanceTracker> PerformanceTrackerPtr;
}  // namespace type
/// @brief Internal helper class that prevent copy constructor for class
///
/// @detail When using this class simply inherit it privately
class NoCopy {
 protected:
  NoCopy(void) {}
 private:
  NoCopy(const NoCopy&);
  NoCopy& operator=(const NoCopy&);
};
/// @brief Internal helper class that makes all default constructors private.
///
/// @detail This prevents initializing class making it static unless an explicit constructor is declared.
/// When using this class simply inherit it privately
class StaticClass {
 private:
  StaticClass(void);
  StaticClass(const StaticClass&);
  StaticClass& operator=(const StaticClass&);
};
}  // namespace base
/// @brief Represents enumeration for severity level used to determine level of logging
///
/// @detail With Easylogging++, developers may disable or enable any level regardless of
/// what the severity is. Or they can choose to log using hierarchical logging flag
enum class Level : base::type::EnumType {
  /// @brief Generic level that represents all the levels. Useful when setting global configuration for all levels
  Global = 1,
  /// @brief Information that can be useful to back-trace certain events - mostly useful than debug logs.
  Trace = 2,
  /// @brief Informational events most useful for developers to debug application
  Debug = 4,
  /// @brief Severe error information that will presumably abort application
  Fatal = 8,
  /// @brief Information representing errors in application but application will keep running
  Error = 16,
  /// @brief Useful when application has potentially harmful situtaions
  Warning = 32,
  /// @brief Information that can be highly useful and vary with verbose logging level.
  Verbose = 64,
  /// @brief Mainly useful to represent current progress of application
  Info = 128,
  /// @brief Represents unknown level
  Unknown = 1010
};
} // namespace el
namespace std {
template<> struct hash<el::Level> {
 public:
  std::size_t operator()(const el::Level& l) const {
    return hash<el::base::type::EnumType> {}(static_cast<el::base::type::EnumType>(l));
  }
};
}
namespace el {
/// @brief Static class that contains helper functions for el::Level
class LevelHelper : base::StaticClass {
 public:
  /// @brief Represents minimum valid level. Useful when iterating through enum.
  static const base::type::EnumType kMinValid = static_cast<base::type::EnumType>(Level::Trace);
  /// @brief Represents maximum valid level. This is used internally and you should not need it.
  static const base::type::EnumType kMaxValid = static_cast<base::type::EnumType>(Level::Info);
  /// @brief Casts level to int, useful for iterating through enum.
  static base::type::EnumType castToInt(Level level) {
    return static_cast<base::type::EnumType>(level);
  }
  /// @brief Casts int(ushort) to level, useful for iterating through enum.
  static Level castFromInt(base::type::EnumType l) {
    return static_cast<Level>(l);
  }
  /// @brief Converts level to associated const char*
  /// @return Upper case string based level.
  static const char* convertToString(Level level);
  /// @brief Converts from levelStr to Level
  /// @param levelStr Upper case string based level.
  ///        Lower case is also valid but providing upper case is recommended.
  static Level convertFromString(const char* levelStr);
  /// @brief Applies specified function to each level starting from startIndex
  /// @param startIndex initial value to start the iteration from. This is passed as pointer and
  ///        is left-shifted so this can be used inside function (fn) to represent current level.
  /// @param fn function to apply with each level. This bool represent whether or not to stop iterating through levels.
  static void forEachLevel(base::type::EnumType* startIndex, const std::function<bool(void)>& fn);
};
/// @brief Represents enumeration of ConfigurationType used to configure or access certain aspect
/// of logging
enum class ConfigurationType : base::type::EnumType {
  /// @brief Determines whether or not corresponding level and logger of logging is enabled
  /// You may disable all logs by using el::Level::Global
  Enabled = 1,
  /// @brief Whether or not to write corresponding log to log file
  ToFile = 2,
  /// @brief Whether or not to write corresponding level and logger log to standard output.
  /// By standard output meaning termnal, command prompt etc
  ToStandardOutput = 4,
  /// @brief Determines format of logging corresponding level and logger.
  Format = 8,
  /// @brief Determines log file (full path) to write logs to for correponding level and logger
  Filename = 16,
  /// @brief Specifies precision of the subsecond part. It should be within range (1-6).
  SubsecondPrecision = 32,
  /// @brief Alias of SubsecondPrecision (for backward compatibility)
  MillisecondsWidth = SubsecondPrecision,
  /// @brief Determines whether or not performance tracking is enabled.
  ///
  /// @detail This does not depend on logger or level. Performance tracking always uses 'performance' logger
  PerformanceTracking = 64,
  /// @brief Specifies log file max size.
  ///
  /// @detail If file size of corresponding log file (for corresponding level) is >= specified size, log file will
  /// be truncated and re-initiated.
  MaxLogFileSize = 128,
  /// @brief Specifies number of log entries to hold until we flush pending log data
  LogFlushThreshold = 256,
  /// @brief Represents unknown configuration
  Unknown = 1010
};
/// @brief Static class that contains helper functions for el::ConfigurationType
class ConfigurationTypeHelper : base::StaticClass {
 public:
  /// @brief Represents minimum valid configuration type. Useful when iterating through enum.
  static const base::type::EnumType kMinValid = static_cast<base::type::EnumType>(ConfigurationType::Enabled);
  /// @brief Represents maximum valid configuration type. This is used internally and you should not need it.
  static const base::type::EnumType kMaxValid = static_cast<base::type::EnumType>(ConfigurationType::MaxLogFileSize);
  /// @brief Casts configuration type to int, useful for iterating through enum.
  static base::type::EnumType castToInt(ConfigurationType configurationType) {
    return static_cast<base::type::EnumType>(configurationType);
  }
  /// @brief Casts int(ushort) to configurationt type, useful for iterating through enum.
  static ConfigurationType castFromInt(base::type::EnumType c) {
    return static_cast<ConfigurationType>(c);
  }
  /// @brief Converts configuration type to associated const char*
  /// @returns Upper case string based configuration type.
  static const char* convertToString(ConfigurationType configurationType);
  /// @brief Converts from configStr to ConfigurationType
  /// @param configStr Upper case string based configuration type.
  ///        Lower case is also valid but providing upper case is recommended.
  static ConfigurationType convertFromString(const char* configStr);
  /// @brief Applies specified function to each configuration type starting from startIndex
  /// @param startIndex initial value to start the iteration from. This is passed by pointer and is left-shifted
  ///        so this can be used inside function (fn) to represent current configuration type.
  /// @param fn function to apply with each configuration type.
  ///        This bool represent whether or not to stop iterating through configurations.
  static inline void forEachConfigType(base::type::EnumType* startIndex, const std::function<bool(void)>& fn);
};
/// @brief Flags used while writing logs. This flags are set by user
enum class LoggingFlag : base::type::EnumType {
  /// @brief Makes sure we have new line for each container log entry
  NewLineForContainer = 1,
  /// @brief Makes sure if -vmodule is used and does not specifies a module, then verbose
  /// logging is allowed via that module.
  AllowVerboseIfModuleNotSpecified = 2,
  /// @brief When handling crashes by default, detailed crash reason will be logged as well
  LogDetailedCrashReason = 4,
  /// @brief Allows to disable application abortion when logged using FATAL level
  DisableApplicationAbortOnFatalLog = 8,
  /// @brief Flushes log with every log-entry (performance sensative) - Disabled by default
  ImmediateFlush = 16,
  /// @brief Enables strict file rolling
  StrictLogFileSizeCheck = 32,
  /// @brief Make terminal output colorful for supported terminals
  ColoredTerminalOutput = 64,
  /// @brief Supports use of multiple logging in same macro, e.g, CLOG(INFO, "default", "network")
  MultiLoggerSupport = 128,
  /// @brief Disables comparing performance tracker's checkpoints
  DisablePerformanceTrackingCheckpointComparison = 256,
  /// @brief Disable VModules
  DisableVModules = 512,
  /// @brief Disable VModules extensions
  DisableVModulesExtensions = 1024,
  /// @brief Enables hierarchical logging
  HierarchicalLogging = 2048,
  /// @brief Creates logger automatically when not available
  CreateLoggerAutomatically = 4096,
  /// @brief Adds spaces b/w logs that separated by left-shift operator
  AutoSpacing = 8192,
  /// @brief Preserves time format and does not convert it to sec, hour etc (performance tracking only)
  FixedTimeFormat = 16384,
  // @brief Ignore SIGINT or crash
  IgnoreSigInt = 32768,
};
namespace base {
/// @brief Namespace containing constants used internally.
namespace consts {
static const char  kFormatSpecifierCharValue               =      'v';
static const char  kFormatSpecifierChar                    =      '%';
static const unsigned int kMaxLogPerCounter                =      100000;
static const unsigned int kMaxLogPerContainer              =      100;
static const unsigned int kDefaultSubsecondPrecision       =      3;

#ifdef ELPP_DEFAULT_LOGGER
static const char* kDefaultLoggerId                        =      ELPP_DEFAULT_LOGGER;
#else
static const char* kDefaultLoggerId                        =      "default";
#endif

#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
#ifdef ELPP_DEFAULT_PERFORMANCE_LOGGER
static const char* kPerformanceLoggerId                    =      ELPP_DEFAULT_PERFORMANCE_LOGGER;
#else
static const char* kPerformanceLoggerId                    =      "performance";
#endif // ELPP_DEFAULT_PERFORMANCE_LOGGER
#endif

#if defined(ELPP_SYSLOG)
static const char* kSysLogLoggerId                         =      "syslog";
#endif  // defined(ELPP_SYSLOG)

#if ELPP_OS_WINDOWS
static const char* kFilePathSeperator                      =      "\\";
#else
static const char* kFilePathSeperator                      =      "/";
#endif  // ELPP_OS_WINDOWS

static const std::size_t kSourceFilenameMaxLength          =      100;
static const std::size_t kSourceLineMaxLength              =      10;
static const Level kPerformanceTrackerDefaultLevel         =      Level::Info;
const struct {
  double value;
  const base::type::char_t* unit;
} kTimeFormats[] = {
  { 1000.0f, ELPP_LITERAL("us") },
  { 1000.0f, ELPP_LITERAL("ms") },
  { 60.0f, ELPP_LITERAL("seconds") },
  { 60.0f, ELPP_LITERAL("minutes") },
  { 24.0f, ELPP_LITERAL("hours") },
  { 7.0f, ELPP_LITERAL("days") }
};
static const int kTimeFormatsCount                           =      sizeof(kTimeFormats) / sizeof(kTimeFormats[0]);
const struct {
  int numb;
  const char* name;
  const char* brief;
  const char* detail;
} kCrashSignals[] = {
  // NOTE: Do not re-order, if you do please check CrashHandler(bool) constructor and CrashHandler::setHandler(..)
  {
    SIGABRT, "SIGABRT", "Abnormal termination",
    "Program was abnormally terminated."
  },
  {
    SIGFPE, "SIGFPE", "Erroneous arithmetic operation",
    "Arithemetic operation issue such as division by zero or operation resulting in overflow."
  },
  {
    SIGILL, "SIGILL", "Illegal instruction",
    "Generally due to a corruption in the code or to an attempt to execute data."
  },
  {
    SIGSEGV, "SIGSEGV", "Invalid access to memory",
    "Program is trying to read an invalid (unallocated, deleted or corrupted) or inaccessible memory."
  },
  {
    SIGINT, "SIGINT", "Interactive attention signal",
    "Interruption generated (generally) by user or operating system."
  },
};
static const int kCrashSignalsCount                          =      sizeof(kCrashSignals) / sizeof(kCrashSignals[0]);
}  // namespace consts
}  // namespace base
typedef std::function<void(const char*, std::size_t)> PreRollOutCallback;
namespace base {
static inline void defaultPreRollOutCallback(const char*, std::size_t) {}
/// @brief Enum to represent timestamp unit
enum class TimestampUnit : base::type::EnumType {
  Microsecond = 0, Millisecond = 1, Second = 2, Minute = 3, Hour = 4, Day = 5
};
/// @brief Format flags used to determine specifiers that are active for performance improvements.
enum class FormatFlags : base::type::EnumType {
  DateTime = 1 << 1,
  LoggerId = 1 << 2,
  File = 1 << 3,
  Line = 1 << 4,
  Location = 1 << 5,
  Function = 1 << 6,
  User = 1 << 7,
  Host = 1 << 8,
  LogMessage = 1 << 9,
  VerboseLevel = 1 << 10,
  AppName = 1 << 11,
  ThreadId = 1 << 12,
  Level = 1 << 13,
  FileBase = 1 << 14,
  LevelShort = 1 << 15
};
/// @brief A subsecond precision class containing actual width and offset of the subsecond part
class SubsecondPrecision {
 public:
  SubsecondPrecision(void) {
    init(base::consts::kDefaultSubsecondPrecision);
  }
  explicit SubsecondPrecision(int width) {
    init(width);
  }
  bool operator==(const SubsecondPrecision& ssPrec) {
    return m_width == ssPrec.m_width && m_offset == ssPrec.m_offset;
  }
  int m_width;
  unsigned int m_offset;
 private:
  void init(int width);
};
/// @brief Type alias of SubsecondPrecision
typedef SubsecondPrecision MillisecondsWidth;
/// @brief Namespace containing utility functions/static classes used internally
namespace utils {
/// @brief Deletes memory safely and points to null
template <typename T>
static
typename std::enable_if<std::is_pointer<T*>::value, void>::type
safeDelete(T*& pointer) {
  if (pointer == nullptr)
    return;
  delete pointer;
  pointer = nullptr;
}
/// @brief Bitwise operations for C++11 strong enum class. This casts e into Flag_T and returns value after bitwise operation
/// Use these function as <pre>flag = bitwise::Or<MyEnum>(MyEnum::val1, flag);</pre>
namespace bitwise {
template <typename Enum>
static inline base::type::EnumType And(Enum e, base::type::EnumType flag) {
  return static_cast<base::type::EnumType>(flag) & static_cast<base::type::EnumType>(e);
}
template <typename Enum>
static inline base::type::EnumType Not(Enum e, base::type::EnumType flag) {
  return static_cast<base::type::EnumType>(flag) & ~(static_cast<base::type::EnumType>(e));
}
template <typename Enum>
static inline base::type::EnumType Or(Enum e, base::type::EnumType flag) {
  return static_cast<base::type::EnumType>(flag) | static_cast<base::type::EnumType>(e);
}
}  // namespace bitwise
template <typename Enum>
static inline void addFlag(Enum e, base::type::EnumType* flag) {
  *flag = base::utils::bitwise::Or<Enum>(e, *flag);
}
template <typename Enum>
static inline void removeFlag(Enum e, base::type::EnumType* flag) {
  *flag = base::utils::bitwise::Not<Enum>(e, *flag);
}
template <typename Enum>
static inline bool hasFlag(Enum e, base::type::EnumType flag) {
  return base::utils::bitwise::And<Enum>(e, flag) > 0x0;
}
}  // namespace utils
namespace threading {
#if ELPP_THREADING_ENABLED
#  if !ELPP_USE_STD_THREADING
namespace internal {
/// @brief A mutex wrapper for compiler that dont yet support std::recursive_mutex
class Mutex : base::NoCopy {
 public:
  Mutex(void) {
#  if ELPP_OS_UNIX
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m_underlyingMutex, &attr);
    pthread_mutexattr_destroy(&attr);
#  elif ELPP_OS_WINDOWS
    InitializeCriticalSection(&m_underlyingMutex);
#  endif  // ELPP_OS_UNIX
  }

  virtual ~Mutex(void) {
#  if ELPP_OS_UNIX
    pthread_mutex_destroy(&m_underlyingMutex);
#  elif ELPP_OS_WINDOWS
    DeleteCriticalSection(&m_underlyingMutex);
#  endif  // ELPP_OS_UNIX
  }

  inline void lock(void) {
#  if ELPP_OS_UNIX
    pthread_mutex_lock(&m_underlyingMutex);
#  elif ELPP_OS_WINDOWS
    EnterCriticalSection(&m_underlyingMutex);
#  endif  // ELPP_OS_UNIX
  }

  inline bool try_lock(void) {
#  if ELPP_OS_UNIX
    return (pthread_mutex_trylock(&m_underlyingMutex) == 0);
#  elif ELPP_OS_WINDOWS
    return TryEnterCriticalSection(&m_underlyingMutex);
#  endif  // ELPP_OS_UNIX
  }

  inline void unlock(void) {
#  if ELPP_OS_UNIX
    pthread_mutex_unlock(&m_underlyingMutex);
#  elif ELPP_OS_WINDOWS
    LeaveCriticalSection(&m_underlyingMutex);
#  endif  // ELPP_OS_UNIX
  }

 private:
#  if ELPP_OS_UNIX
  pthread_mutex_t m_underlyingMutex;
#  elif ELPP_OS_WINDOWS
  CRITICAL_SECTION m_underlyingMutex;
#  endif  // ELPP_OS_UNIX
};
/// @brief Scoped lock for compiler that dont yet support std::lock_guard
template <typename M>
class ScopedLock : base::NoCopy {
 public:
  explicit ScopedLock(M& mutex) {
    m_mutex = &mutex;
    m_mutex->lock();
  }

  virtual ~ScopedLock(void) {
    m_mutex->unlock();
  }
 private:
  M* m_mutex;
  ScopedLock(void);
};
} // namespace internal
typedef base::threading::internal::Mutex Mutex;
typedef base::threading::internal::ScopedLock<base::threading::Mutex> ScopedLock;
#  else
typedef std::recursive_mutex Mutex;
typedef std::lock_guard<base::threading::Mutex> ScopedLock;
#  endif  // !ELPP_USE_STD_THREADING
#else
namespace internal {
/// @brief Mutex wrapper used when multi-threading is disabled.
class NoMutex : base::NoCopy {
 public:
  NoMutex(void) {}
  inline void lock(void) {}
  inline bool try_lock(void) {
    return true;
  }
  inline void unlock(void) {}
};
/// @brief Lock guard wrapper used when multi-threading is disabled.
template <typename Mutex>
class NoScopedLock : base::NoCopy {
 public:
  explicit NoScopedLock(Mutex&) {
  }
  virtual ~NoScopedLock(void) {
  }
 private:
  NoScopedLock(void);
};
}  // namespace internal
typedef base::threading::internal::NoMutex Mutex;
typedef base::threading::internal::NoScopedLock<base::threading::Mutex> ScopedLock;
#endif  // ELPP_THREADING_ENABLED
/// @brief Base of thread safe class, this class is inheritable-only
class ThreadSafe {
 public:
  virtual inline void acquireLock(void) ELPP_FINAL { m_mutex.lock(); }
  virtual inline void releaseLock(void) ELPP_FINAL { m_mutex.unlock(); }
  virtual inline base::threading::Mutex& lock(void) ELPP_FINAL { return m_mutex; }
 protected:
  ThreadSafe(void) {}
  virtual ~ThreadSafe(void) {}
 private:
  base::threading::Mutex m_mutex;
};

#if ELPP_THREADING_ENABLED
#  if !ELPP_USE_STD_THREADING
/// @brief Gets ID of currently running threading in windows systems. On unix, nothing is returned.
static std::string getCurrentThreadId(void) {
  std::stringstream ss;
#      if (ELPP_OS_WINDOWS)
  ss << GetCurrentThreadId();
#      endif  // (ELPP_OS_WINDOWS)
  return ss.str();
}
#  else
/// @brief Gets ID of currently running threading using std::this_thread::get_id()
static std::string getCurrentThreadId(void) {
  std::stringstream ss;
  ss << std::this_thread::get_id();
  return ss.str();
}
#  endif  // !ELPP_USE_STD_THREADING
#else
static inline std::string getCurrentThreadId(void) {
  return std::string();
}
#endif  // ELPP_THREADING_ENABLED
}  // namespace threading
namespace utils {
class File : base::StaticClass {
 public:
  /// @brief Creates new out file stream for specified filename.
  /// @return Pointer to newly created fstream or nullptr
  static base::type::fstream_t* newFileStream(const std::string& filename);

  /// @brief Gets size of file provided in stream
  static std::size_t getSizeOfFile(base::type::fstream_t* fs);

  /// @brief Determines whether or not provided path exist in current file system
  static bool pathExists(const char* path, bool considerFile = false);

  /// @brief Creates specified path on file system
  /// @param path Path to create.
  static bool createPath(const std::string& path);
  /// @brief Extracts path of filename with leading slash
  static std::string extractPathFromFilename(const std::string& fullPath,
      const char* seperator = base::consts::kFilePathSeperator);
  /// @brief builds stripped filename and puts it in buff
  static void buildStrippedFilename(const char* filename, char buff[],
                                    std::size_t limit = base::consts::kSourceFilenameMaxLength);
  /// @brief builds base filename and puts it in buff
  static void buildBaseFilename(const std::string& fullPath, char buff[],
                                std::size_t limit = base::consts::kSourceFilenameMaxLength,
                                const char* seperator = base::consts::kFilePathSeperator);
};
/// @brief String utilities helper class used internally. You should not use it.
class Str : base::StaticClass {
 public:
  /// @brief Checks if character is digit. Dont use libc implementation of it to prevent locale issues.
  static inline bool isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  /// @brief Matches wildcards, '*' and '?' only supported.
  static bool wildCardMatch(const char* str, const char* pattern);

  static std::string& ltrim(std::string& str);
  static std::string& rtrim(std::string& str);
  static std::string& trim(std::string& str);

  /// @brief Determines whether or not str starts with specified string
  /// @param str String to check
  /// @param start String to check against
  /// @return Returns true if starts with specified string, false otherwise
  static bool startsWith(const std::string& str, const std::string& start);

  /// @brief Determines whether or not str ends with specified string
  /// @param str String to check
  /// @param end String to check against
  /// @return Returns true if ends with specified string, false otherwise
  static bool endsWith(const std::string& str, const std::string& end);

  /// @brief Replaces all instances of replaceWhat with 'replaceWith'. Original variable is changed for performance.
  /// @param [in,out] str String to replace from
  /// @param replaceWhat Character to replace
  /// @param replaceWith Character to replace with
  /// @return Modified version of str
  static std::string& replaceAll(std::string& str, char replaceWhat, char replaceWith);

  /// @brief Replaces all instances of 'replaceWhat' with 'replaceWith'. (String version) Replaces in place
  /// @param str String to replace from
  /// @param replaceWhat Character to replace
  /// @param replaceWith Character to replace with
  /// @return Modified (original) str
  static std::string& replaceAll(std::string& str, const std::string& replaceWhat,
                                 const std::string& replaceWith);

  static void replaceFirstWithEscape(base::type::string_t& str, const base::type::string_t& replaceWhat,
                                     const base::type::string_t& replaceWith);
#if defined(ELPP_UNICODE)
  static void replaceFirstWithEscape(base::type::string_t& str, const base::type::string_t& replaceWhat,
                                     const std::string& replaceWith);
#endif  // defined(ELPP_UNICODE)
  /// @brief Converts string to uppercase
  /// @param str String to convert
  /// @return Uppercase string
  static std::string& toUpper(std::string& str);

  /// @brief Compares cstring equality - uses strcmp
  static bool cStringEq(const char* s1, const char* s2);

  /// @brief Compares cstring equality (case-insensitive) - uses toupper(char)
  /// Dont use strcasecmp because of CRT (VC++)
  static bool cStringCaseEq(const char* s1, const char* s2);

  /// @brief Returns true if c exist in str
  static bool contains(const char* str, char c);

  static char* convertAndAddToBuff(std::size_t n, int len, char* buf, const char* bufLim, bool zeroPadded = true);
  static char* addToBuff(const char* str, char* buf, const char* bufLim);
  static char* clearBuff(char buff[], std::size_t lim);

  /// @brief Converst wchar* to char*
  ///        NOTE: Need to free return value after use!
  static char* wcharPtrToCharPtr(const wchar_t* line);
};
/// @brief Operating System helper static class used internally. You should not use it.
class OS : base::StaticClass {
 public:
#if ELPP_OS_WINDOWS
  /// @brief Gets environment variables for Windows based OS.
  ///        We are not using <code>getenv(const char*)</code> because of CRT deprecation
  /// @param varname Variable name to get environment variable value for
  /// @return If variable exist the value of it otherwise nullptr
  static const char* getWindowsEnvironmentVariable(const char* varname);
#endif  // ELPP_OS_WINDOWS
#if ELPP_OS_ANDROID
  /// @brief Reads android property value
  static std::string getProperty(const char* prop);

  /// @brief Reads android device name
  static std::string getDeviceName(void);
#endif  // ELPP_OS_ANDROID

  /// @brief Runs command on terminal and returns the output.
  ///
  /// @detail This is applicable only on unix based systems, for all other OS, an empty string is returned.
  /// @param command Bash command
  /// @return Result of bash output or empty string if no result found.
  static const std::string getBashOutput(const char* command);

  /// @brief Gets environment variable. This is cross-platform and CRT safe (for VC++)
  /// @param variableName Environment variable name
  /// @param defaultVal If no environment variable or value found the value to return by default
  /// @param alternativeBashCommand If environment variable not found what would be alternative bash command
  ///        in order to look for value user is looking for. E.g, for 'user' alternative command will 'whoami'
  static std::string getEnvironmentVariable(const char* variableName, const char* defaultVal,
      const char* alternativeBashCommand = nullptr);
  /// @brief Gets current username.
  static std::string currentUser(void);

  /// @brief Gets current host name or computer name.
  ///
  /// @detail For android systems this is device name with its manufacturer and model seperated by hyphen
  static std::string currentHost(void);
  /// @brief Whether or not terminal supports colors
  static bool termSupportsColor(void);
};
/// @brief Contains utilities for cross-platform date/time. This class make use of el::base::utils::Str
class DateTime : base::StaticClass {
 public:
  /// @brief Cross platform gettimeofday for Windows and unix platform. This can be used to determine current microsecond.
  ///
  /// @detail For unix system it uses gettimeofday(timeval*, timezone*) and for Windows, a seperate implementation is provided
  /// @param [in,out] tv Pointer that gets updated
  static void gettimeofday(struct timeval* tv);

  /// @brief Gets current date and time with a subsecond part.
  /// @param format User provided date/time format
  /// @param ssPrec A pointer to base::SubsecondPrecision from configuration (non-null)
  /// @returns string based date time in specified format.
  static std::string getDateTime(const char* format, const base::SubsecondPrecision* ssPrec);

  /// @brief Converts timeval (struct from ctime) to string using specified format and subsecond precision
  static std::string timevalToString(struct timeval tval, const char* format,
                                     const el::base::SubsecondPrecision* ssPrec);

  /// @brief Formats time to get unit accordingly, units like second if > 1000 or minutes if > 60000 etc
  static base::type::string_t formatTime(unsigned long long time, base::TimestampUnit timestampUnit);

  /// @brief Gets time difference in milli/micro second depending on timestampUnit
  static unsigned long long getTimeDifference(const struct timeval& endTime, const struct timeval& startTime,
      base::TimestampUnit timestampUnit);


  static struct ::tm* buildTimeInfo(struct timeval* currTime, struct ::tm* timeInfo);
 private:
  static char* parseFormat(char* buf, std::size_t bufSz, const char* format, const struct tm* tInfo,
                           std::size_t msec, const base::SubsecondPrecision* ssPrec);
};
/// @brief Command line arguments for application if specified using el::Helpers::setArgs(..) or START_EASYLOGGINGPP(..)
class CommandLineArgs {
 public:
  CommandLineArgs(void) {
    setArgs(0, static_cast<char**>(nullptr));
  }
  CommandLineArgs(int argc, const char** argv) {
    setArgs(argc, argv);
  }
  CommandLineArgs(int argc, char** argv) {
    setArgs(argc, argv);
  }
  virtual ~CommandLineArgs(void) {}
  /// @brief Sets arguments and parses them
  inline void setArgs(int argc, const char** argv) {
    setArgs(argc, const_cast<char**>(argv));
  }
  /// @brief Sets arguments and parses them
  void setArgs(int argc, char** argv);
  /// @brief Returns true if arguments contain paramKey with a value (seperated by '=')
  bool hasParamWithValue(const char* paramKey) const;
  /// @brief Returns value of arguments
  /// @see hasParamWithValue(const char*)
  const char* getParamValue(const char* paramKey) const;
  /// @brief Return true if arguments has a param (not having a value) i,e without '='
  bool hasParam(const char* paramKey) const;
  /// @brief Returns true if no params available. This exclude argv[0]
  bool empty(void) const;
  /// @brief Returns total number of arguments. This exclude argv[0]
  std::size_t size(void) const;
  friend base::type::ostream_t& operator<<(base::type::ostream_t& os, const CommandLineArgs& c);

 private:
  int m_argc;
  char** m_argv;
  std::unordered_map<std::string, std::string> m_paramsWithValue;
  std::vector<std::string> m_params;
};
/// @brief Abstract registry (aka repository) that provides basic interface for pointer repository specified by T_Ptr type.
///
/// @detail Most of the functions are virtual final methods but anything implementing this abstract class should implement
/// unregisterAll() and deepCopy(const AbstractRegistry<T_Ptr, Container>&) and write registerNew() method according to container
/// and few more methods; get() to find element, unregister() to unregister single entry.
/// Please note that this is thread-unsafe and should also implement thread-safety mechanisms in implementation.
template <typename T_Ptr, typename Container>
class AbstractRegistry : public base::threading::ThreadSafe {
 public:
  typedef typename Container::iterator iterator;
  typedef typename Container::const_iterator const_iterator;

  /// @brief Default constructor
  AbstractRegistry(void) {}

  /// @brief Move constructor that is useful for base classes
  AbstractRegistry(AbstractRegistry&& sr) {
    if (this == &sr) {
      return;
    }
    unregisterAll();
    m_list = std::move(sr.m_list);
  }

  bool operator==(const AbstractRegistry<T_Ptr, Container>& other) {
    if (size() != other.size()) {
      return false;
    }
    for (std::size_t i = 0; i < m_list.size(); ++i) {
      if (m_list.at(i) != other.m_list.at(i)) {
        return false;
      }
    }
    return true;
  }

  bool operator!=(const AbstractRegistry<T_Ptr, Container>& other) {
    if (size() != other.size()) {
      return true;
    }
    for (std::size_t i = 0; i < m_list.size(); ++i) {
      if (m_list.at(i) != other.m_list.at(i)) {
        return true;
      }
    }
    return false;
  }

  /// @brief Assignment move operator
  AbstractRegistry& operator=(AbstractRegistry&& sr) {
    if (this == &sr) {
      return *this;
    }
    unregisterAll();
    m_list = std::move(sr.m_list);
    return *this;
  }

  virtual ~AbstractRegistry(void) {
  }

  /// @return Iterator pointer from start of repository
  virtual inline iterator begin(void) ELPP_FINAL {
    return m_list.begin();
  }

  /// @return Iterator pointer from end of repository
  virtual inline iterator end(void) ELPP_FINAL {
    return m_list.end();
  }


  /// @return Constant iterator pointer from start of repository
  virtual inline const_iterator cbegin(void) const ELPP_FINAL {
    return m_list.cbegin();
  }

  /// @return End of repository
  virtual inline const_iterator cend(void) const ELPP_FINAL {
    return m_list.cend();
  }

  /// @return Whether or not repository is empty
  virtual inline bool empty(void) const ELPP_FINAL {
    return m_list.empty();
  }

  /// @return Size of repository
  virtual inline std::size_t size(void) const ELPP_FINAL {
    return m_list.size();
  }

  /// @brief Returns underlying container by reference
  virtual inline Container& list(void) ELPP_FINAL {
    return m_list;
  }

  /// @brief Returns underlying container by constant reference.
  virtual inline const Container& list(void) const ELPP_FINAL {
    return m_list;
  }

  /// @brief Unregisters all the pointers from current repository.
  virtual void unregisterAll(void) = 0;

 protected:
  virtual void deepCopy(const AbstractRegistry<T_Ptr, Container>&) = 0;
  void reinitDeepCopy(const AbstractRegistry<T_Ptr, Container>& sr) {
    unregisterAll();
    deepCopy(sr);
  }

 private:
  Container m_list;
};

/// @brief A pointer registry mechanism to manage memory and provide search functionalities. (non-predicate version)
///
/// @detail NOTE: This is thread-unsafe implementation (although it contains lock function, it does not use these functions)
///         of AbstractRegistry<T_Ptr, Container>. Any implementation of this class should be
///         explicitly (by using lock functions)
template <typename T_Ptr, typename T_Key = const char*>
class Registry : public AbstractRegistry<T_Ptr, std::unordered_map<T_Key, T_Ptr*>> {
 public:
  typedef typename Registry<T_Ptr, T_Key>::iterator iterator;
  typedef typename Registry<T_Ptr, T_Key>::const_iterator const_iterator;

  Registry(void) {}

  /// @brief Copy constructor that is useful for base classes. Try to avoid this constructor, use move constructor.
  Registry(const Registry& sr) : AbstractRegistry<T_Ptr, std::vector<T_Ptr*>>() {
    if (this == &sr) {
      return;
    }
    this->reinitDeepCopy(sr);
  }

  /// @brief Assignment operator that unregisters all the existing registeries and deeply copies each of repo element
  /// @see unregisterAll()
  /// @see deepCopy(const AbstractRegistry&)
  Registry& operator=(const Registry& sr) {
    if (this == &sr) {
      return *this;
    }
    this->reinitDeepCopy(sr);
    return *this;
  }

  virtual ~Registry(void) {
    unregisterAll();
  }

 protected:
  virtual void unregisterAll(void) ELPP_FINAL {
    if (!this->empty()) {
      for (auto&& curr : this->list()) {
        base::utils::safeDelete(curr.second);
      }
      this->list().clear();
    }
  }

/// @brief Registers new registry to repository.
  virtual void registerNew(const T_Key& uniqKey, T_Ptr* ptr) ELPP_FINAL {
    unregister(uniqKey);
    this->list().insert(std::make_pair(uniqKey, ptr));
  }

/// @brief Unregisters single entry mapped to specified unique key
  void unregister(const T_Key& uniqKey) {
    T_Ptr* existing = get(uniqKey);
    if (existing != nullptr) {
      this->list().erase(uniqKey);
      base::utils::safeDelete(existing);
    }
  }

/// @brief Gets pointer from repository. If none found, nullptr is returned.
  T_Ptr* get(const T_Key& uniqKey) {
    iterator it = this->list().find(uniqKey);
    return it == this->list().end()
           ? nullptr
           : it->second;
  }

 private:
  virtual void deepCopy(const AbstractRegistry<T_Ptr, std::unordered_map<T_Key, T_Ptr*>>& sr) ELPP_FINAL {
    for (const_iterator it = sr.cbegin(); it != sr.cend(); ++it) {
      registerNew(it->first, new T_Ptr(*it->second));
    }
  }
};

/// @brief A pointer registry mechanism to manage memory and provide search functionalities. (predicate version)
///
/// @detail NOTE: This is thread-unsafe implementation of AbstractRegistry<T_Ptr, Container>. Any implementation of this class
/// should be made thread-safe explicitly
template <typename T_Ptr, typename Pred>
class RegistryWithPred : public AbstractRegistry<T_Ptr, std::vector<T_Ptr*>> {
 public:
  typedef typename RegistryWithPred<T_Ptr, Pred>::iterator iterator;
  typedef typename RegistryWithPred<T_Ptr, Pred>::const_iterator const_iterator;

  RegistryWithPred(void) {
  }

  virtual ~RegistryWithPred(void) {
    unregisterAll();
  }

  /// @brief Copy constructor that is useful for base classes. Try to avoid this constructor, use move constructor.
  RegistryWithPred(const RegistryWithPred& sr) : AbstractRegistry<T_Ptr, std::vector<T_Ptr*>>() {
    if (this == &sr) {
      return;
    }
    this->reinitDeepCopy(sr);
  }

  /// @brief Assignment operator that unregisters all the existing registeries and deeply copies each of repo element
  /// @see unregisterAll()
  /// @see deepCopy(const AbstractRegistry&)
  RegistryWithPred& operator=(const RegistryWithPred& sr) {
    if (this == &sr) {
      return *this;
    }
    this->reinitDeepCopy(sr);
    return *this;
  }

  friend base::type::ostream_t& operator<<(base::type::ostream_t& os, const RegistryWithPred& sr) {
    for (const_iterator it = sr.list().begin(); it != sr.list().end(); ++it) {
      os << ELPP_LITERAL("    ") << **it << ELPP_LITERAL("\n");
    }
    return os;
  }

 protected:
  virtual void unregisterAll(void) ELPP_FINAL {
    if (!this->empty()) {
      for (auto&& curr : this->list()) {
        base::utils::safeDelete(curr);
      }
      this->list().clear();
    }
  }

  virtual void unregister(T_Ptr*& ptr) ELPP_FINAL {
    if (ptr) {
      iterator iter = this->begin();
      for (; iter != this->end(); ++iter) {
        if (ptr == *iter) {
          break;
        }
      }
      if (iter != this->end() && *iter != nullptr) {
        this->list().erase(iter);
        base::utils::safeDelete(*iter);
      }
    }
  }

  virtual inline void registerNew(T_Ptr* ptr) ELPP_FINAL {
    this->list().push_back(ptr);
  }

/// @brief Gets pointer from repository with speicifed arguments. Arguments are passed to predicate
/// in order to validate pointer.
  template <typename T, typename T2>
  T_Ptr* get(const T& arg1, const T2 arg2) {
    iterator iter = std::find_if(this->list().begin(), this->list().end(), Pred(arg1, arg2));
    if (iter != this->list().end() && *iter != nullptr) {
      return *iter;
    }
    return nullptr;
  }

 private:
  virtual void deepCopy(const AbstractRegistry<T_Ptr, std::vector<T_Ptr*>>& sr) {
    for (const_iterator it = sr.list().begin(); it != sr.list().end(); ++it) {
      registerNew(new T_Ptr(**it));
    }
  }
};
class Utils {
 public:
  template <typename T, typename TPtr>
  static bool installCallback(const std::string& id, std::unordered_map<std::string, TPtr>* mapT) {
    if (mapT->find(id) == mapT->end()) {
      mapT->insert(std::make_pair(id, TPtr(new T())));
      return true;
    }
    return false;
  }

  template <typename T, typename TPtr>
  static void uninstallCallback(const std::string& id, std::unordered_map<std::string, TPtr>* mapT) {
    if (mapT->find(id) != mapT->end()) {
      mapT->erase(id);
    }
  }

  template <typename T, typename TPtr>
  static T* callback(const std::string& id, std::unordered_map<std::string, TPtr>* mapT) {
    typename std::unordered_map<std::string, TPtr>::iterator iter = mapT->find(id);
    if (iter != mapT->end()) {
      return static_cast<T*>(iter->second.get());
    }
    return nullptr;
  }
};
}  // namespace utils
} // namespace base
/// @brief Base of Easylogging++ friendly class
///
/// @detail After inheriting this class publicly, implement pure-virtual function `void log(std::ostream&) const`
class Loggable {
 public:
  virtual ~Loggable(void) {}
  virtual void log(el::base::type::ostream_t&) const = 0;
 private:
  friend inline el::base::type::ostream_t& operator<<(el::base::type::ostream_t& os, const Loggable& loggable) {
    loggable.log(os);
    return os;
  }
};
namespace base {
/// @brief Represents log format containing flags and date format. This is used internally to start initial log
class LogFormat : public Loggable {
 public:
  LogFormat(void);
  LogFormat(Level level, const base::type::string_t& format);
  LogFormat(const LogFormat& logFormat);
  LogFormat(LogFormat&& logFormat);
  LogFormat& operator=(const LogFormat& logFormat);
  virtual ~LogFormat(void) {}
  bool operator==(const LogFormat& other);

  /// @brief Updates format to be used while logging.
  /// @param userFormat User provided format
  void parseFromFormat(const base::type::string_t& userFormat);

  inline Level level(void) const {
    return m_level;
  }

  inline const base::type::string_t& userFormat(void) const {
    return m_userFormat;
  }

  inline const base::type::string_t& format(void) const {
    return m_format;
  }

  inline const std::string& dateTimeFormat(void) const {
    return m_dateTimeFormat;
  }

  inline base::type::EnumType flags(void) const {
    return m_flags;
  }

  inline bool hasFlag(base::FormatFlags flag) const {
    return base::utils::hasFlag(flag, m_flags);
  }

  virtual void log(el::base::type::ostream_t& os) const {
    os << m_format;
  }

 protected:
  /// @brief Updates date time format if available in currFormat.
  /// @param index Index where %datetime, %date or %time was found
  /// @param [in,out] currFormat current format that is being used to format
  virtual void updateDateFormat(std::size_t index, base::type::string_t& currFormat) ELPP_FINAL;

  /// @brief Updates %level from format. This is so that we dont have to do it at log-writing-time. It uses m_format and m_level
  virtual void updateFormatSpec(void) ELPP_FINAL;

  inline void addFlag(base::FormatFlags flag) {
    base::utils::addFlag(flag, &m_flags);
  }

 private:
  Level m_level;
  base::type::string_t m_userFormat;
  base::type::string_t m_format;
  std::string m_dateTimeFormat;
  base::type::EnumType m_flags;
  std::string m_currentUser;
  std::string m_currentHost;
  friend class el::Logger;  // To resolve loggerId format specifier easily
};
}  // namespace base
/// @brief Resolving function for format specifier
typedef std::function<std::string(const LogMessage*)> FormatSpecifierValueResolver;
/// @brief User-provided custom format specifier
/// @see el::Helpers::installCustomFormatSpecifier
/// @see FormatSpecifierValueResolver
class CustomFormatSpecifier {
 public:
  CustomFormatSpecifier(const char* formatSpecifier, const FormatSpecifierValueResolver& resolver) :
    m_formatSpecifier(formatSpecifier), m_resolver(resolver) {}
  inline const char* formatSpecifier(void) const {
    return m_formatSpecifier;
  }
  inline const FormatSpecifierValueResolver& resolver(void) const {
    return m_resolver;
  }
  inline bool operator==(const char* formatSpecifier) {
    return strcmp(m_formatSpecifier, formatSpecifier) == 0;
  }

 private:
  const char* m_formatSpecifier;
  FormatSpecifierValueResolver m_resolver;
};
/// @brief Represents single configuration that has representing level, configuration type and a string based value.
///
/// @detail String based value means any value either its boolean, integer or string itself, it will be embedded inside quotes
/// and will be parsed later.
///
/// Consider some examples below:
///   * el::Configuration confEnabledInfo(el::Level::Info, el::ConfigurationType::Enabled, "true");
///   * el::Configuration confMaxLogFileSizeInfo(el::Level::Info, el::ConfigurationType::MaxLogFileSize, "2048");
///   * el::Configuration confFilenameInfo(el::Level::Info, el::ConfigurationType::Filename, "/var/log/my.log");
class Configuration : public Loggable {
 public:
  Configuration(const Configuration& c);
  Configuration& operator=(const Configuration& c);

  virtual ~Configuration(void) {
  }

  /// @brief Full constructor used to sets value of configuration
  Configuration(Level level, ConfigurationType configurationType, const std::string& value);

  /// @brief Gets level of current configuration
  inline Level level(void) const {
    return m_level;
  }

  /// @brief Gets configuration type of current configuration
  inline ConfigurationType configurationType(void) const {
    return m_configurationType;
  }

  /// @brief Gets string based configuration value
  inline const std::string& value(void) const {
    return m_value;
  }

  /// @brief Set string based configuration value
  /// @param value Value to set. Values have to be std::string; For boolean values use "true", "false", for any integral values
  ///        use them in quotes. They will be parsed when configuring
  inline void setValue(const std::string& value) {
    m_value = value;
  }

  virtual void log(el::base::type::ostream_t& os) const;

  /// @brief Used to find configuration from configuration (pointers) repository. Avoid using it.
  class Predicate {
   public:
    Predicate(Level level, ConfigurationType configurationType);

    bool operator()(const Configuration* conf) const;

   private:
    Level m_level;
    ConfigurationType m_configurationType;
  };

 private:
  Level m_level;
  ConfigurationType m_configurationType;
  std::string m_value;
};

/// @brief Thread-safe Configuration repository
///
/// @detail This repository represents configurations for all the levels and configuration type mapped to a value.
class Configurations : public base::utils::RegistryWithPred<Configuration, Configuration::Predicate> {
 public:
  /// @brief Default constructor with empty repository
  Configurations(void);

  /// @brief Constructor used to set configurations using configuration file.
  /// @param configurationFile Full path to configuration file
  /// @param useDefaultsForRemaining Lets you set the remaining configurations to default.
  /// @param base If provided, this configuration will be based off existing repository that this argument is pointing to.
  /// @see parseFromFile(const std::string&, Configurations* base)
  /// @see setRemainingToDefault()
  Configurations(const std::string& configurationFile, bool useDefaultsForRemaining = true,
                 Configurations* base = nullptr);

  virtual ~Configurations(void) {
  }

  /// @brief Parses configuration from file.
  /// @param configurationFile Full path to configuration file
  /// @param base Configurations to base new configuration repository off. This value is used when you want to use
  ///        existing Configurations to base all the values and then set rest of configuration via configuration file.
  /// @return True if successfully parsed, false otherwise. You may define 'ELPP_DEBUG_ASSERT_FAILURE' to make sure you
  ///         do not proceed without successful parse.
  bool parseFromFile(const std::string& configurationFile, Configurations* base = nullptr);

  /// @brief Parse configurations from configuration string.
  ///
  /// @detail This configuration string has same syntax as configuration file contents. Make sure all the necessary
  /// new line characters are provided.
  /// @param base Configurations to base new configuration repository off. This value is used when you want to use
  ///        existing Configurations to base all the values and then set rest of configuration via configuration text.
  /// @return True if successfully parsed, false otherwise. You may define 'ELPP_DEBUG_ASSERT_FAILURE' to make sure you
  ///         do not proceed without successful parse.
  bool parseFromText(const std::string& configurationsString, Configurations* base = nullptr);

  /// @brief Sets configuration based-off an existing configurations.
  /// @param base Pointer to existing configurations.
  void setFromBase(Configurations* base);

  /// @brief Determines whether or not specified configuration type exists in the repository.
  ///
  /// @detail Returns as soon as first level is found.
  /// @param configurationType Type of configuration to check existence for.
  bool hasConfiguration(ConfigurationType configurationType);

  /// @brief Determines whether or not specified configuration type exists for specified level
  /// @param level Level to check
  /// @param configurationType Type of configuration to check existence for.
  bool hasConfiguration(Level level, ConfigurationType configurationType);

  /// @brief Sets value of configuration for specified level.
  ///
  /// @detail Any existing configuration for specified level will be replaced. Also note that configuration types
  /// ConfigurationType::SubsecondPrecision and ConfigurationType::PerformanceTracking will be ignored if not set for
  /// Level::Global because these configurations are not dependant on level.
  /// @param level Level to set configuration for (el::Level).
  /// @param configurationType Type of configuration (el::ConfigurationType)
  /// @param value A string based value. Regardless of what the data type of configuration is, it will always be string
  /// from users' point of view. This is then parsed later to be used internally.
  /// @see Configuration::setValue(const std::string& value)
  /// @see el::Level
  /// @see el::ConfigurationType
  void set(Level level, ConfigurationType configurationType, const std::string& value);

  /// @brief Sets single configuration based on other single configuration.
  /// @see set(Level level, ConfigurationType configurationType, const std::string& value)
  void set(Configuration* conf);

  inline Configuration* get(Level level, ConfigurationType configurationType) {
    base::threading::ScopedLock scopedLock(lock());
    return RegistryWithPred<Configuration, Configuration::Predicate>::get(level, configurationType);
  }

  /// @brief Sets configuration for all levels.
  /// @param configurationType Type of configuration
  /// @param value String based value
  /// @see Configurations::set(Level level, ConfigurationType configurationType, const std::string& value)
  inline void setGlobally(ConfigurationType configurationType, const std::string& value) {
    setGlobally(configurationType, value, false);
  }

  /// @brief Clears repository so that all the configurations are unset
  inline void clear(void) {
    base::threading::ScopedLock scopedLock(lock());
    unregisterAll();
  }

  /// @brief Gets configuration file used in parsing this configurations.
  ///
  /// @detail If this repository was set manually or by text this returns empty string.
  inline const std::string& configurationFile(void) const {
    return m_configurationFile;
  }

  /// @brief Sets configurations to "factory based" configurations.
  void setToDefault(void);

  /// @brief Lets you set the remaining configurations to default.
  ///
  /// @detail By remaining, it means that the level/type a configuration does not exist for.
  /// This function is useful when you want to minimize chances of failures, e.g, if you have a configuration file that sets
  /// configuration for all the configurations except for Enabled or not, we use this so that ENABLED is set to default i.e,
  /// true. If you dont do this explicitly (either by calling this function or by using second param in Constructor
  /// and try to access a value, an error is thrown
  void setRemainingToDefault(void);

  /// @brief Parser used internally to parse configurations from file or text.
  ///
  /// @detail This class makes use of base::utils::Str.
  /// You should not need this unless you are working on some tool for Easylogging++
  class Parser : base::StaticClass {
   public:
    /// @brief Parses configuration from file.
    /// @param configurationFile Full path to configuration file
    /// @param sender Sender configurations pointer. Usually 'this' is used from calling class
    /// @param base Configurations to base new configuration repository off. This value is used when you want to use
    ///        existing Configurations to base all the values and then set rest of configuration via configuration file.
    /// @return True if successfully parsed, false otherwise. You may define '_STOP_ON_FIRSTELPP_ASSERTION' to make sure you
    ///         do not proceed without successful parse.
    static bool parseFromFile(const std::string& configurationFile, Configurations* sender,
                              Configurations* base = nullptr);

    /// @brief Parse configurations from configuration string.
    ///
    /// @detail This configuration string has same syntax as configuration file contents. Make sure all the necessary
    /// new line characters are provided. You may define '_STOP_ON_FIRSTELPP_ASSERTION' to make sure you
    /// do not proceed without successful parse (This is recommended)
    /// @param configurationsString the configuration in plain text format
    /// @param sender Sender configurations pointer. Usually 'this' is used from calling class
    /// @param base Configurations to base new configuration repository off. This value is used when you want to use
    ///        existing Configurations to base all the values and then set rest of configuration via configuration text.
    /// @return True if successfully parsed, false otherwise.
    static bool parseFromText(const std::string& configurationsString, Configurations* sender,
                              Configurations* base = nullptr);

   private:
    friend class el::Loggers;
    static void ignoreComments(std::string* line);
    static bool isLevel(const std::string& line);
    static bool isComment(const std::string& line);
    static inline bool isConfig(const std::string& line);
    static bool parseLine(std::string* line, std::string* currConfigStr, std::string* currLevelStr, Level* currLevel,
                          Configurations* conf);
  };

 private:
  std::string m_configurationFile;
  bool m_isFromFile;
  friend class el::Loggers;

  /// @brief Unsafely sets configuration if does not already exist
  void unsafeSetIfNotExist(Level level, ConfigurationType configurationType, const std::string& value);

  /// @brief Thread unsafe set
  void unsafeSet(Level level, ConfigurationType configurationType, const std::string& value);

  /// @brief Sets configurations for all levels including Level::Global if includeGlobalLevel is true
  /// @see Configurations::setGlobally(ConfigurationType configurationType, const std::string& value)
  void setGlobally(ConfigurationType configurationType, const std::string& value, bool includeGlobalLevel);

  /// @brief Sets configurations (Unsafely) for all levels including Level::Global if includeGlobalLevel is true
  /// @see Configurations::setGlobally(ConfigurationType configurationType, const std::string& value)
  void unsafeSetGlobally(ConfigurationType configurationType, const std::string& value, bool includeGlobalLevel);
};

namespace base {
typedef std::shared_ptr<base::type::fstream_t> FileStreamPtr;
typedef std::unordered_map<std::string, FileStreamPtr> LogStreamsReferenceMap;
/// @brief Configurations with data types.
///
/// @detail el::Configurations have string based values. This is whats used internally in order to read correct configurations.
/// This is to perform faster while writing logs using correct configurations.
///
/// This is thread safe and final class containing non-virtual destructor (means nothing should inherit this class)
class TypedConfigurations : public base::threading::ThreadSafe {
 public:
  /// @brief Constructor to initialize (construct) the object off el::Configurations
  /// @param configurations Configurations pointer/reference to base this typed configurations off.
  /// @param logStreamsReference Use ELPP->registeredLoggers()->logStreamsReference()
  TypedConfigurations(Configurations* configurations, base::LogStreamsReferenceMap* logStreamsReference);

  TypedConfigurations(const TypedConfigurations& other);

  virtual ~TypedConfigurations(void) {
  }

  const Configurations* configurations(void) const {
    return m_configurations;
  }

  bool enabled(Level level);
  bool toFile(Level level);
  const std::string& filename(Level level);
  bool toStandardOutput(Level level);
  const base::LogFormat& logFormat(Level level);
  const base::SubsecondPrecision& subsecondPrecision(Level level = Level::Global);
  const base::MillisecondsWidth& millisecondsWidth(Level level = Level::Global);
  bool performanceTracking(Level level = Level::Global);
  base::type::fstream_t* fileStream(Level level);
  std::size_t maxLogFileSize(Level level);
  std::size_t logFlushThreshold(Level level);

 private:
  Configurations* m_configurations;
  std::unordered_map<Level, bool> m_enabledMap;
  std::unordered_map<Level, bool> m_toFileMap;
  std::unordered_map<Level, std::string> m_filenameMap;
  std::unordered_map<Level, bool> m_toStandardOutputMap;
  std::unordered_map<Level, base::LogFormat> m_logFormatMap;
  std::unordered_map<Level, base::SubsecondPrecision> m_subsecondPrecisionMap;
  std::unordered_map<Level, bool> m_performanceTrackingMap;
  std::unordered_map<Level, base::FileStreamPtr> m_fileStreamMap;
  std::unordered_map<Level, std::size_t> m_maxLogFileSizeMap;
  std::unordered_map<Level, std::size_t> m_logFlushThresholdMap;
  base::LogStreamsReferenceMap* m_logStreamsReference;

  friend class el::Helpers;
  friend class el::base::MessageBuilder;
  friend class el::base::Writer;
  friend class el::base::DefaultLogDispatchCallback;
  friend class el::base::LogDispatcher;

  template <typename Conf_T>
  inline Conf_T getConfigByVal(Level level, const std::unordered_map<Level, Conf_T>* confMap, const char* confName) {
    base::threading::ScopedLock scopedLock(lock());
    return unsafeGetConfigByVal(level, confMap, confName);  // This is not unsafe anymore - mutex locked in scope
  }

  template <typename Conf_T>
  inline Conf_T& getConfigByRef(Level level, std::unordered_map<Level, Conf_T>* confMap, const char* confName) {
    base::threading::ScopedLock scopedLock(lock());
    return unsafeGetConfigByRef(level, confMap, confName);  // This is not unsafe anymore - mutex locked in scope
  }

  template <typename Conf_T>
  Conf_T unsafeGetConfigByVal(Level level, const std::unordered_map<Level, Conf_T>* confMap, const char* confName) {
    ELPP_UNUSED(confName);
    typename std::unordered_map<Level, Conf_T>::const_iterator it = confMap->find(level);
    if (it == confMap->end()) {
      try {
        return confMap->at(Level::Global);
      } catch (...) {
        ELPP_INTERNAL_ERROR("Unable to get configuration [" << confName << "] for level ["
                            << LevelHelper::convertToString(level) << "]"
                            << std::endl << "Please ensure you have properly configured logger.", false);
        return Conf_T();
      }
    }
    return it->second;
  }

  template <typename Conf_T>
  Conf_T& unsafeGetConfigByRef(Level level, std::unordered_map<Level, Conf_T>* confMap, const char* confName) {
    ELPP_UNUSED(confName);
    typename std::unordered_map<Level, Conf_T>::iterator it = confMap->find(level);
    if (it == confMap->end()) {
      try {
        return confMap->at(Level::Global);
      } catch (...) {
        ELPP_INTERNAL_ERROR("Unable to get configuration [" << confName << "] for level ["
                            << LevelHelper::convertToString(level) << "]"
                            << std::endl << "Please ensure you have properly configured logger.", false);
      }
    }
    return it->second;
  }

  template <typename Conf_T>
  void setValue(Level level, const Conf_T& value, std::unordered_map<Level, Conf_T>* confMap,
                bool includeGlobalLevel = true) {
    // If map is empty and we are allowed to add into generic level (Level::Global), do it!
    if (confMap->empty() && includeGlobalLevel) {
      confMap->insert(std::make_pair(Level::Global, value));
      return;
    }
    // If same value exist in generic level already, dont add it to explicit level
    typename std::unordered_map<Level, Conf_T>::iterator it = confMap->find(Level::Global);
    if (it != confMap->end() && it->second == value) {
      return;
    }
    // Now make sure we dont double up values if we really need to add it to explicit level
    it = confMap->find(level);
    if (it == confMap->end()) {
      // Value not found for level, add new
      confMap->insert(std::make_pair(level, value));
    } else {
      // Value found, just update value
      confMap->at(level) = value;
    }
  }

  void build(Configurations* configurations);
  unsigned long getULong(std::string confVal);
  std::string resolveFilename(const std::string& filename);
  void insertFile(Level level, const std::string& fullFilename);
  bool unsafeValidateFileRolling(Level level, const PreRollOutCallback& preRollOutCallback);

  inline bool validateFileRolling(Level level, const PreRollOutCallback& preRollOutCallback) {
    base::threading::ScopedLock scopedLock(lock());
    return unsafeValidateFileRolling(level, preRollOutCallback);
  }
};
/// @brief Class that keeps record of current line hit for occasional logging
class HitCounter {
 public:
  HitCounter(void) :
    m_filename(""),
    m_lineNumber(0),
    m_hitCounts(0) {
  }

  HitCounter(const char* filename, base::type::LineNumber lineNumber) :
    m_filename(filename),
    m_lineNumber(lineNumber),
    m_hitCounts(0) {
  }

  HitCounter(const HitCounter& hitCounter) :
    m_filename(hitCounter.m_filename),
    m_lineNumber(hitCounter.m_lineNumber),
    m_hitCounts(hitCounter.m_hitCounts) {
  }

  HitCounter& operator=(const HitCounter& hitCounter) {
    if (&hitCounter != this) {
      m_filename = hitCounter.m_filename;
      m_lineNumber = hitCounter.m_lineNumber;
      m_hitCounts = hitCounter.m_hitCounts;
    }
    return *this;
  }

  virtual ~HitCounter(void) {
  }

  /// @brief Resets location of current hit counter
  inline void resetLocation(const char* filename, base::type::LineNumber lineNumber) {
    m_filename = filename;
    m_lineNumber = lineNumber;
  }

  /// @brief Validates hit counts and resets it if necessary
  inline void validateHitCounts(std::size_t n) {
    if (m_hitCounts >= base::consts::kMaxLogPerCounter) {
      m_hitCounts = (n >= 1 ? base::consts::kMaxLogPerCounter % n : 0);
    }
    ++m_hitCounts;
  }

  inline const char* filename(void) const {
    return m_filename;
  }

  inline base::type::LineNumber lineNumber(void) const {
    return m_lineNumber;
  }

  inline std::size_t hitCounts(void) const {
    return m_hitCounts;
  }

  inline void increment(void) {
    ++m_hitCounts;
  }

  class Predicate {
   public:
    Predicate(const char* filename, base::type::LineNumber lineNumber)
      : m_filename(filename),
        m_lineNumber(lineNumber) {
    }
    inline bool operator()(const HitCounter* counter) {
      return ((counter != nullptr) &&
              (strcmp(counter->m_filename, m_filename) == 0) &&
              (counter->m_lineNumber == m_lineNumber));
    }

   private:
    const char* m_filename;
    base::type::LineNumber m_lineNumber;
  };

 private:
  const char* m_filename;
  base::type::LineNumber m_lineNumber;
  std::size_t m_hitCounts;
};
/// @brief Repository for hit counters used across the application
class RegisteredHitCounters : public base::utils::RegistryWithPred<base::HitCounter, base::HitCounter::Predicate> {
 public:
  /// @brief Validates counter for every N, i.e, registers new if does not exist otherwise updates original one
  /// @return True if validation resulted in triggering hit. Meaning logs should be written everytime true is returned
  bool validateEveryN(const char* filename, base::type::LineNumber lineNumber, std::size_t n);

  /// @brief Validates counter for hits >= N, i.e, registers new if does not exist otherwise updates original one
  /// @return True if validation resulted in triggering hit. Meaning logs should be written everytime true is returned
  bool validateAfterN(const char* filename, base::type::LineNumber lineNumber, std::size_t n);

  /// @brief Validates counter for hits are <= n, i.e, registers new if does not exist otherwise updates original one
  /// @return True if validation resulted in triggering hit. Meaning logs should be written everytime true is returned
  bool validateNTimes(const char* filename, base::type::LineNumber lineNumber, std::size_t n);

  /// @brief Gets hit counter registered at specified position
  inline const base::HitCounter* getCounter(const char* filename, base::type::LineNumber lineNumber) {
    base::threading::ScopedLock scopedLock(lock());
    return get(filename, lineNumber);
  }
};
/// @brief Action to be taken for dispatching
enum class DispatchAction : base::type::EnumType {
  None = 1, NormalLog = 2, SysLog = 4
};
}  // namespace base
template <typename T>
class Callback : protected base::threading::ThreadSafe {
 public:
  Callback(void) : m_enabled(true) {}
  inline bool enabled(void) const {
    return m_enabled;
  }
  inline void setEnabled(bool enabled) {
    base::threading::ScopedLock scopedLock(lock());
    m_enabled = enabled;
  }
 protected:
  virtual void handle(const T* handlePtr) = 0;
 private:
  bool m_enabled;
};
class LogDispatchData {
 public:
  LogDispatchData() : m_logMessage(nullptr), m_dispatchAction(base::DispatchAction::None) {}
  inline const LogMessage* logMessage(void) const {
    return m_logMessage;
  }
  inline base::DispatchAction dispatchAction(void) const {
    return m_dispatchAction;
  }
  inline void setLogMessage(LogMessage* logMessage) {
    m_logMessage = logMessage;
  }
  inline void setDispatchAction(base::DispatchAction dispatchAction) {
    m_dispatchAction = dispatchAction;
  }
 private:
  LogMessage* m_logMessage;
  base::DispatchAction m_dispatchAction;
  friend class base::LogDispatcher;

};
class LogDispatchCallback : public Callback<LogDispatchData> {
 protected:
  virtual void handle(const LogDispatchData* data);
  base::threading::Mutex& fileHandle(const LogDispatchData* data);
 private:
  friend class base::LogDispatcher;
  std::unordered_map<std::string, std::unique_ptr<base::threading::Mutex>> m_fileLocks;
  base::threading::Mutex m_fileLocksMapLock;
};
class PerformanceTrackingCallback : public Callback<PerformanceTrackingData> {
 private:
  friend class base::PerformanceTracker;
};
class LoggerRegistrationCallback : public Callback<Logger> {
 private:
  friend class base::RegisteredLoggers;
};
class LogBuilder : base::NoCopy {
 public:
  LogBuilder() : m_termSupportsColor(base::utils::OS::termSupportsColor()) {}
  virtual ~LogBuilder(void) {
    ELPP_INTERNAL_INFO(3, "Destroying log builder...")
  }
  virtual base::type::string_t build(const LogMessage* logMessage, bool appendNewLine) const = 0;
  void convertToColoredOutput(base::type::string_t* logLine, Level level);
 private:
  bool m_termSupportsColor;
  friend class el::base::DefaultLogDispatchCallback;
};
typedef std::shared_ptr<LogBuilder> LogBuilderPtr;
/// @brief Represents a logger holding ID and configurations we need to write logs
///
/// @detail This class does not write logs itself instead its used by writer to read configuations from.
class Logger : public base::threading::ThreadSafe, public Loggable {
 public:
  Logger(const std::string& id, base::LogStreamsReferenceMap* logStreamsReference);
  Logger(const std::string& id, const Configurations& configurations, base::LogStreamsReferenceMap* logStreamsReference);
  Logger(const Logger& logger);
  Logger& operator=(const Logger& logger);

  virtual ~Logger(void) {
    base::utils::safeDelete(m_typedConfigurations);
  }

  virtual inline void log(el::base::type::ostream_t& os) const {
    os << m_id.c_str();
  }

  /// @brief Configures the logger using specified configurations.
  void configure(const Configurations& configurations);

  /// @brief Reconfigures logger using existing configurations
  void reconfigure(void);

  inline const std::string& id(void) const {
    return m_id;
  }

  inline const std::string& parentApplicationName(void) const {
    return m_parentApplicationName;
  }

  inline void setParentApplicationName(const std::string& parentApplicationName) {
    m_parentApplicationName = parentApplicationName;
  }

  inline Configurations* configurations(void) {
    return &m_configurations;
  }

  inline base::TypedConfigurations* typedConfigurations(void) {
    return m_typedConfigurations;
  }

  static bool isValidId(const std::string& id);

  /// @brief Flushes logger to sync all log files for all levels
  void flush(void);

  void flush(Level level, base::type::fstream_t* fs);

  inline bool isFlushNeeded(Level level) {
    return ++m_unflushedCount.find(level)->second >= m_typedConfigurations->logFlushThreshold(level);
  }

  inline LogBuilder* logBuilder(void) const {
    return m_logBuilder.get();
  }

  inline void setLogBuilder(const LogBuilderPtr& logBuilder) {
    m_logBuilder = logBuilder;
  }

  inline bool enabled(Level level) const {
    return m_typedConfigurations->enabled(level);
  }

#if ELPP_VARIADIC_TEMPLATES_SUPPORTED
#  define LOGGER_LEVEL_WRITERS_SIGNATURES(FUNCTION_NAME)\
template <typename T, typename... Args>\
inline void FUNCTION_NAME(const char*, const T&, const Args&...);\
template <typename T>\
inline void FUNCTION_NAME(const T&);

  template <typename T, typename... Args>
  inline void verbose(int, const char*, const T&, const Args&...);

  template <typename T>
  inline void verbose(int, const T&);

  LOGGER_LEVEL_WRITERS_SIGNATURES(info)
  LOGGER_LEVEL_WRITERS_SIGNATURES(debug)
  LOGGER_LEVEL_WRITERS_SIGNATURES(warn)
  LOGGER_LEVEL_WRITERS_SIGNATURES(error)
  LOGGER_LEVEL_WRITERS_SIGNATURES(fatal)
  LOGGER_LEVEL_WRITERS_SIGNATURES(trace)
#  undef LOGGER_LEVEL_WRITERS_SIGNATURES
#endif // ELPP_VARIADIC_TEMPLATES_SUPPORTED
 private:
  std::string m_id;
  base::TypedConfigurations* m_typedConfigurations;
  base::type::stringstream_t m_stream;
  std::string m_parentApplicationName;
  bool m_isConfigured;
  Configurations m_configurations;
  std::unordered_map<Level, unsigned int> m_unflushedCount;
  base::LogStreamsReferenceMap* m_logStreamsReference;
  LogBuilderPtr m_logBuilder;

  friend class el::LogMessage;
  friend class el::Loggers;
  friend class el::Helpers;
  friend class el::base::RegisteredLoggers;
  friend class el::base::DefaultLogDispatchCallback;
  friend class el::base::MessageBuilder;
  friend class el::base::Writer;
  friend class el::base::PErrorWriter;
  friend class el::base::Storage;
  friend class el::base::PerformanceTracker;
  friend class el::base::LogDispatcher;

  Logger(void);

#if ELPP_VARIADIC_TEMPLATES_SUPPORTED
  template <typename T, typename... Args>
  void log_(Level, int, const char*, const T&, const Args&...);

  template <typename T>
  inline void log_(Level, int, const T&);

  template <typename T, typename... Args>
  void log(Level, const char*, const T&, const Args&...);

  template <typename T>
  inline void log(Level, const T&);
#endif // ELPP_VARIADIC_TEMPLATES_SUPPORTED

  void initUnflushedCount(void);

  inline base::type::stringstream_t& stream(void) {
    return m_stream;
  }

  void resolveLoggerFormatSpec(void) const;
};
namespace base {
/// @brief Loggers repository
class RegisteredLoggers : public base::utils::Registry<Logger, std::string> {
 public:
  explicit RegisteredLoggers(const LogBuilderPtr& defaultLogBuilder);

  virtual ~RegisteredLoggers(void) {
    unsafeFlushAll();
  }

  inline void setDefaultConfigurations(const Configurations& configurations) {
    base::threading::ScopedLock scopedLock(lock());
    m_defaultConfigurations.setFromBase(const_cast<Configurations*>(&configurations));
  }

  inline Configurations* defaultConfigurations(void) {
    return &m_defaultConfigurations;
  }

  Logger* get(const std::string& id, bool forceCreation = true);

  template <typename T>
  inline bool installLoggerRegistrationCallback(const std::string& id) {
    return base::utils::Utils::installCallback<T, base::type::LoggerRegistrationCallbackPtr>(id,
           &m_loggerRegistrationCallbacks);
  }

  template <typename T>
  inline void uninstallLoggerRegistrationCallback(const std::string& id) {
    base::utils::Utils::uninstallCallback<T, base::type::LoggerRegistrationCallbackPtr>(id, &m_loggerRegistrationCallbacks);
  }

  template <typename T>
  inline T* loggerRegistrationCallback(const std::string& id) {
    return base::utils::Utils::callback<T, base::type::LoggerRegistrationCallbackPtr>(id, &m_loggerRegistrationCallbacks);
  }

  bool remove(const std::string& id);

  inline bool has(const std::string& id) {
    return get(id, false) != nullptr;
  }

  inline void unregister(Logger*& logger) {
    base::threading::ScopedLock scopedLock(lock());
    base::utils::Registry<Logger, std::string>::unregister(logger->id());
  }

  inline base::LogStreamsReferenceMap* logStreamsReference(void) {
    return &m_logStreamsReference;
  }

  inline void flushAll(void) {
    base::threading::ScopedLock scopedLock(lock());
    unsafeFlushAll();
  }

  inline void setDefaultLogBuilder(LogBuilderPtr& logBuilderPtr) {
    base::threading::ScopedLock scopedLock(lock());
    m_defaultLogBuilder = logBuilderPtr;
  }

 private:
  LogBuilderPtr m_defaultLogBuilder;
  Configurations m_defaultConfigurations;
  base::LogStreamsReferenceMap m_logStreamsReference;
  std::unordered_map<std::string, base::type::LoggerRegistrationCallbackPtr> m_loggerRegistrationCallbacks;
  friend class el::base::Storage;

  void unsafeFlushAll(void);
};
/// @brief Represents registries for verbose logging
class VRegistry : base::NoCopy, public base::threading::ThreadSafe {
 public:
  explicit VRegistry(base::type::VerboseLevel level, base::type::EnumType* pFlags);

  /// @brief Sets verbose level. Accepted range is 0-9
  void setLevel(base::type::VerboseLevel level);

  inline base::type::VerboseLevel level(void) const {
    return m_level;
  }

  inline void clearModules(void) {
    base::threading::ScopedLock scopedLock(lock());
    m_modules.clear();
  }

  void setModules(const char* modules);

  bool allowed(base::type::VerboseLevel vlevel, const char* file);

  inline const std::unordered_map<std::string, base::type::VerboseLevel>& modules(void) const {
    return m_modules;
  }

  void setFromArgs(const base::utils::CommandLineArgs* commandLineArgs);

  /// @brief Whether or not vModules enabled
  inline bool vModulesEnabled(void) {
    return !base::utils::hasFlag(LoggingFlag::DisableVModules, *m_pFlags);
  }

 private:
  base::type::VerboseLevel m_level;
  base::type::EnumType* m_pFlags;
  std::unordered_map<std::string, base::type::VerboseLevel> m_modules;
};
}  // namespace base
class LogMessage {
 public:
  LogMessage(Level level, const std::string& file, base::type::LineNumber line, const std::string& func,
             base::type::VerboseLevel verboseLevel, Logger* logger) :
    m_level(level), m_file(file), m_line(line), m_func(func),
    m_verboseLevel(verboseLevel), m_logger(logger), m_message(logger->stream().str()) {
  }
  inline Level level(void) const {
    return m_level;
  }
  inline const std::string& file(void) const {
    return m_file;
  }
  inline base::type::LineNumber line(void) const {
    return m_line;
  }
  inline const std::string& func(void) const {
    return m_func;
  }
  inline base::type::VerboseLevel verboseLevel(void) const {
    return m_verboseLevel;
  }
  inline Logger* logger(void) const {
    return m_logger;
  }
  inline const base::type::string_t& message(void) const {
    return m_message;
  }
 private:
  Level m_level;
  std::string m_file;
  base::type::LineNumber m_line;
  std::string m_func;
  base::type::VerboseLevel m_verboseLevel;
  Logger* m_logger;
  base::type::string_t m_message;
};
namespace base {
#if ELPP_ASYNC_LOGGING
class AsyncLogItem {
 public:
  explicit AsyncLogItem(const LogMessage& logMessage, const LogDispatchData& data, const base::type::string_t& logLine)
    : m_logMessage(logMessage), m_dispatchData(data), m_logLine(logLine) {}
  virtual ~AsyncLogItem() {}
  inline LogMessage* logMessage(void) {
    return &m_logMessage;
  }
  inline LogDispatchData* data(void) {
    return &m_dispatchData;
  }
  inline base::type::string_t logLine(void) {
    return m_logLine;
  }
 private:
  LogMessage m_logMessage;
  LogDispatchData m_dispatchData;
  base::type::string_t m_logLine;
};
class AsyncLogQueue : public base::threading::ThreadSafe {
 public:
  virtual ~AsyncLogQueue() {
    ELPP_INTERNAL_INFO(6, "~AsyncLogQueue");
  }

  inline AsyncLogItem next(void) {
    base::threading::ScopedLock scopedLock(lock());
    AsyncLogItem result = m_queue.front();
    m_queue.pop();
    return result;
  }

  inline void push(const AsyncLogItem& item) {
    base::threading::ScopedLock scopedLock(lock());
    m_queue.push(item);
  }
  inline void pop(void) {
    base::threading::ScopedLock scopedLock(lock());
    m_queue.pop();
  }
  inline AsyncLogItem front(void) {
    base::threading::ScopedLock scopedLock(lock());
    return m_queue.front();
  }
  inline bool empty(void) {
    base::threading::ScopedLock scopedLock(lock());
    return m_queue.empty();
  }
 private:
  std::queue<AsyncLogItem> m_queue;
};
class IWorker {
 public:
  virtual ~IWorker() {}
  virtual void start() = 0;
};
#endif // ELPP_ASYNC_LOGGING
/// @brief Easylogging++ management storage
class Storage : base::NoCopy, public base::threading::ThreadSafe {
 public:
#if ELPP_ASYNC_LOGGING
  Storage(const LogBuilderPtr& defaultLogBuilder, base::IWorker* asyncDispatchWorker);
#else
  explicit Storage(const LogBuilderPtr& defaultLogBuilder);
#endif  // ELPP_ASYNC_LOGGING

  virtual ~Storage(void);

  inline bool validateEveryNCounter(const char* filename, base::type::LineNumber lineNumber, std::size_t occasion) {
    return hitCounters()->validateEveryN(filename, lineNumber, occasion);
  }

  inline bool validateAfterNCounter(const char* filename, base::type::LineNumber lineNumber, std::size_t n) {
    return hitCounters()->validateAfterN(filename, lineNumber, n);
  }

  inline bool validateNTimesCounter(const char* filename, base::type::LineNumber lineNumber, std::size_t n) {
    return hitCounters()->validateNTimes(filename, lineNumber, n);
  }

  inline base::RegisteredHitCounters* hitCounters(void) const {
    return m_registeredHitCounters;
  }

  inline base::RegisteredLoggers* registeredLoggers(void) const {
    return m_registeredLoggers;
  }

  inline base::VRegistry* vRegistry(void) const {
    return m_vRegistry;
  }

#if ELPP_ASYNC_LOGGING
  inline base::AsyncLogQueue* asyncLogQueue(void) const {
    return m_asyncLogQueue;
  }
#endif  // ELPP_ASYNC_LOGGING

  inline const base::utils::CommandLineArgs* commandLineArgs(void) const {
    return &m_commandLineArgs;
  }

  inline void addFlag(LoggingFlag flag) {
    base::utils::addFlag(flag, &m_flags);
  }

  inline void removeFlag(LoggingFlag flag) {
    base::utils::removeFlag(flag, &m_flags);
  }

  inline bool hasFlag(LoggingFlag flag) const {
    return base::utils::hasFlag(flag, m_flags);
  }

  inline base::type::EnumType flags(void) const {
    return m_flags;
  }

  inline void setFlags(base::type::EnumType flags) {
    m_flags = flags;
  }

  inline void setPreRollOutCallback(const PreRollOutCallback& callback) {
    m_preRollOutCallback = callback;
  }

  inline void unsetPreRollOutCallback(void) {
    m_preRollOutCallback = base::defaultPreRollOutCallback;
  }

  inline PreRollOutCallback& preRollOutCallback(void) {
    return m_preRollOutCallback;
  }

  bool hasCustomFormatSpecifier(const char* formatSpecifier);
  void installCustomFormatSpecifier(const CustomFormatSpecifier& customFormatSpecifier);
  bool uninstallCustomFormatSpecifier(const char* formatSpecifier);

  const std::vector<CustomFormatSpecifier>* customFormatSpecifiers(void) const {
    return &m_customFormatSpecifiers;
  }

  base::threading::Mutex& customFormatSpecifiersLock() {
    return m_customFormatSpecifiersLock;
  }

  inline void setLoggingLevel(Level level) {
    m_loggingLevel = level;
  }

  template <typename T>
  inline bool installLogDispatchCallback(const std::string& id) {
    return base::utils::Utils::installCallback<T, base::type::LogDispatchCallbackPtr>(id, &m_logDispatchCallbacks);
  }

  template <typename T>
  inline void uninstallLogDispatchCallback(const std::string& id) {
    base::utils::Utils::uninstallCallback<T, base::type::LogDispatchCallbackPtr>(id, &m_logDispatchCallbacks);
  }
  template <typename T>
  inline T* logDispatchCallback(const std::string& id) {
    return base::utils::Utils::callback<T, base::type::LogDispatchCallbackPtr>(id, &m_logDispatchCallbacks);
  }

#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
  template <typename T>
  inline bool installPerformanceTrackingCallback(const std::string& id) {
    return base::utils::Utils::installCallback<T, base::type::PerformanceTrackingCallbackPtr>(id,
           &m_performanceTrackingCallbacks);
  }

  template <typename T>
  inline void uninstallPerformanceTrackingCallback(const std::string& id) {
    base::utils::Utils::uninstallCallback<T, base::type::PerformanceTrackingCallbackPtr>(id,
        &m_performanceTrackingCallbacks);
  }

  template <typename T>
  inline T* performanceTrackingCallback(const std::string& id) {
    return base::utils::Utils::callback<T, base::type::PerformanceTrackingCallbackPtr>(id, &m_performanceTrackingCallbacks);
  }
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)

  /// @brief Sets thread name for current thread. Requires std::thread
  inline void setThreadName(const std::string& name) {
    if (name.empty()) return;
    base::threading::ScopedLock scopedLock(m_threadNamesLock);
    m_threadNames[base::threading::getCurrentThreadId()] = name;
  }

  inline std::string getThreadName(const std::string& threadId) {
    base::threading::ScopedLock scopedLock(m_threadNamesLock);
    std::unordered_map<std::string, std::string>::const_iterator it = m_threadNames.find(threadId);
    if (it == m_threadNames.end()) {
      return threadId;
    }
    return it->second;
  }
 private:
  base::RegisteredHitCounters* m_registeredHitCounters;
  base::RegisteredLoggers* m_registeredLoggers;
  base::type::EnumType m_flags;
  base::VRegistry* m_vRegistry;
#if ELPP_ASYNC_LOGGING
  base::AsyncLogQueue* m_asyncLogQueue;
  base::IWorker* m_asyncDispatchWorker;
#endif  // ELPP_ASYNC_LOGGING
  base::utils::CommandLineArgs m_commandLineArgs;
  PreRollOutCallback m_preRollOutCallback;
  std::unordered_map<std::string, base::type::LogDispatchCallbackPtr> m_logDispatchCallbacks;
  std::unordered_map<std::string, base::type::PerformanceTrackingCallbackPtr> m_performanceTrackingCallbacks;
  std::unordered_map<std::string, std::string> m_threadNames;
  std::vector<CustomFormatSpecifier> m_customFormatSpecifiers;
  base::threading::Mutex m_customFormatSpecifiersLock;
  base::threading::Mutex m_threadNamesLock;
  Level m_loggingLevel;

  friend class el::Helpers;
  friend class el::base::DefaultLogDispatchCallback;
  friend class el::LogBuilder;
  friend class el::base::MessageBuilder;
  friend class el::base::Writer;
  friend class el::base::PerformanceTracker;
  friend class el::base::LogDispatcher;

  void setApplicationArguments(int argc, char** argv);

  inline void setApplicationArguments(int argc, const char** argv) {
    setApplicationArguments(argc, const_cast<char**>(argv));
  }
};
extern ELPP_EXPORT base::type::StoragePointer elStorage;
#define ELPP el::base::elStorage
class DefaultLogDispatchCallback : public LogDispatchCallback {
 protected:
  void handle(const LogDispatchData* data);
 private:
  const LogDispatchData* m_data;
  void dispatch(base::type::string_t&& logLine);
};
#if ELPP_ASYNC_LOGGING
class AsyncLogDispatchCallback : public LogDispatchCallback {
 protected:
  void handle(const LogDispatchData* data);
};
class AsyncDispatchWorker : public base::IWorker, public base::threading::ThreadSafe {
 public:
  AsyncDispatchWorker();
  virtual ~AsyncDispatchWorker();

  bool clean(void);
  void emptyQueue(void);
  virtual void start(void);
  void handle(AsyncLogItem* logItem);
  void run(void);

  void setContinueRunning(bool value) {
    base::threading::ScopedLock scopedLock(m_continueRunningLock);
    m_continueRunning = value;
  }

  bool continueRunning(void) const {
    return m_continueRunning;
  }
 private:
  std::condition_variable cv;
  bool m_continueRunning;
  base::threading::Mutex m_continueRunningLock;
};
#endif  // ELPP_ASYNC_LOGGING
}  // namespace base
namespace base {
class DefaultLogBuilder : public LogBuilder {
 public:
  base::type::string_t build(const LogMessage* logMessage, bool appendNewLine) const;
};
/// @brief Dispatches log messages
class LogDispatcher : base::NoCopy {
 public:
  LogDispatcher(bool proceed, LogMessage* logMessage, base::DispatchAction dispatchAction) :
    m_proceed(proceed),
    m_logMessage(logMessage),
    m_dispatchAction(std::move(dispatchAction)) {
  }

  void dispatch(void);

 private:
  bool m_proceed;
  LogMessage* m_logMessage;
  base::DispatchAction m_dispatchAction;
};
#if defined(ELPP_STL_LOGGING)
/// @brief Workarounds to write some STL logs
///
/// @detail There is workaround needed to loop through some stl containers. In order to do that, we need iterable containers
/// of same type and provide iterator interface and pass it on to writeIterator().
/// Remember, this is passed by value in constructor so that we dont change original containers.
/// This operation is as expensive as Big-O(std::min(class_.size(), base::consts::kMaxLogPerContainer))
namespace workarounds {
/// @brief Abstract IterableContainer template that provides interface for iterable classes of type T
template <typename T, typename Container>
class IterableContainer {
 public:
  typedef typename Container::iterator iterator;
  typedef typename Container::const_iterator const_iterator;
  IterableContainer(void) {}
  virtual ~IterableContainer(void) {}
  iterator begin(void) {
    return getContainer().begin();
  }
  iterator end(void) {
    return getContainer().end();
  }
 private:
  virtual Container& getContainer(void) = 0;
};
/// @brief Implements IterableContainer and provides iterable std::priority_queue class
template<typename T, typename Container = std::vector<T>, typename Comparator = std::less<typename Container::value_type>>
class IterablePriorityQueue : public IterableContainer<T, Container>,
  public std::priority_queue<T, Container, Comparator> {
 public:
  IterablePriorityQueue(std::priority_queue<T, Container, Comparator> queue_) {
    std::size_t count_ = 0;
    while (++count_ < base::consts::kMaxLogPerContainer && !queue_.empty()) {
      this->push(queue_.top());
      queue_.pop();
    }
  }
 private:
  inline Container& getContainer(void) {
    return this->c;
  }
};
/// @brief Implements IterableContainer and provides iterable std::queue class
template<typename T, typename Container = std::deque<T>>
class IterableQueue : public IterableContainer<T, Container>, public std::queue<T, Container> {
 public:
  IterableQueue(std::queue<T, Container> queue_) {
    std::size_t count_ = 0;
    while (++count_ < base::consts::kMaxLogPerContainer && !queue_.empty()) {
      this->push(queue_.front());
      queue_.pop();
    }
  }
 private:
  inline Container& getContainer(void) {
    return this->c;
  }
};
/// @brief Implements IterableContainer and provides iterable std::stack class
template<typename T, typename Container = std::deque<T>>
class IterableStack : public IterableContainer<T, Container>, public std::stack<T, Container> {
 public:
  IterableStack(std::stack<T, Container> stack_) {
    std::size_t count_ = 0;
    while (++count_ < base::consts::kMaxLogPerContainer && !stack_.empty()) {
      this->push(stack_.top());
      stack_.pop();
    }
  }
 private:
  inline Container& getContainer(void) {
    return this->c;
  }
};
}  // namespace workarounds
#endif  // defined(ELPP_STL_LOGGING)
// Log message builder
class MessageBuilder {
 public:
  MessageBuilder(void) : m_logger(nullptr), m_containerLogSeperator(ELPP_LITERAL("")) {}
  void initialize(Logger* logger);

#  define ELPP_SIMPLE_LOG(LOG_TYPE)\
MessageBuilder& operator<<(LOG_TYPE msg) {\
m_logger->stream() << msg;\
if (ELPP->hasFlag(LoggingFlag::AutoSpacing)) {\
m_logger->stream() << " ";\
}\
return *this;\
}

  inline MessageBuilder& operator<<(const std::string& msg) {
    return operator<<(msg.c_str());
  }
  ELPP_SIMPLE_LOG(char)
  ELPP_SIMPLE_LOG(bool)
  ELPP_SIMPLE_LOG(signed short)
  ELPP_SIMPLE_LOG(unsigned short)
  ELPP_SIMPLE_LOG(signed int)
  ELPP_SIMPLE_LOG(unsigned int)
  ELPP_SIMPLE_LOG(signed long)
  ELPP_SIMPLE_LOG(unsigned long)
  ELPP_SIMPLE_LOG(float)
  ELPP_SIMPLE_LOG(double)
  ELPP_SIMPLE_LOG(char*)
  ELPP_SIMPLE_LOG(const char*)
  ELPP_SIMPLE_LOG(const void*)
  ELPP_SIMPLE_LOG(long double)
  inline MessageBuilder& operator<<(const std::wstring& msg) {
    return operator<<(msg.c_str());
  }
  MessageBuilder& operator<<(const wchar_t* msg);
  // ostream manipulators
  inline MessageBuilder& operator<<(std::ostream& (*OStreamMani)(std::ostream&)) {
    m_logger->stream() << OStreamMani;
    return *this;
  }
#define ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(temp)                                                    \
template <typename T>                                                                            \
inline MessageBuilder& operator<<(const temp<T>& template_inst) {                                \
return writeIterator(template_inst.begin(), template_inst.end(), template_inst.size());      \
}
#define ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(temp)                                                    \
template <typename T1, typename T2>                                                              \
inline MessageBuilder& operator<<(const temp<T1, T2>& template_inst) {                           \
return writeIterator(template_inst.begin(), template_inst.end(), template_inst.size());      \
}
#define ELPP_ITERATOR_CONTAINER_LOG_THREE_ARG(temp)                                                  \
template <typename T1, typename T2, typename T3>                                                 \
inline MessageBuilder& operator<<(const temp<T1, T2, T3>& template_inst) {                       \
return writeIterator(template_inst.begin(), template_inst.end(), template_inst.size());      \
}
#define ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(temp)                                                   \
template <typename T1, typename T2, typename T3, typename T4>                                    \
inline MessageBuilder& operator<<(const temp<T1, T2, T3, T4>& template_inst) {                   \
return writeIterator(template_inst.begin(), template_inst.end(), template_inst.size());      \
}
#define ELPP_ITERATOR_CONTAINER_LOG_FIVE_ARG(temp)                                                   \
template <typename T1, typename T2, typename T3, typename T4, typename T5>                       \
inline MessageBuilder& operator<<(const temp<T1, T2, T3, T4, T5>& template_inst) {               \
return writeIterator(template_inst.begin(), template_inst.end(), template_inst.size());      \
}

#if defined(ELPP_STL_LOGGING)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(std::vector)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(std::list)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(std::deque)
  ELPP_ITERATOR_CONTAINER_LOG_THREE_ARG(std::set)
  ELPP_ITERATOR_CONTAINER_LOG_THREE_ARG(std::multiset)
  ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(std::map)
  ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(std::multimap)
  template <class T, class Container>
  inline MessageBuilder& operator<<(const std::queue<T, Container>& queue_) {
    base::workarounds::IterableQueue<T, Container> iterableQueue_ =
      static_cast<base::workarounds::IterableQueue<T, Container> >(queue_);
    return writeIterator(iterableQueue_.begin(), iterableQueue_.end(), iterableQueue_.size());
  }
  template <class T, class Container>
  inline MessageBuilder& operator<<(const std::stack<T, Container>& stack_) {
    base::workarounds::IterableStack<T, Container> iterableStack_ =
      static_cast<base::workarounds::IterableStack<T, Container> >(stack_);
    return writeIterator(iterableStack_.begin(), iterableStack_.end(), iterableStack_.size());
  }
  template <class T, class Container, class Comparator>
  inline MessageBuilder& operator<<(const std::priority_queue<T, Container, Comparator>& priorityQueue_) {
    base::workarounds::IterablePriorityQueue<T, Container, Comparator> iterablePriorityQueue_ =
      static_cast<base::workarounds::IterablePriorityQueue<T, Container, Comparator> >(priorityQueue_);
    return writeIterator(iterablePriorityQueue_.begin(), iterablePriorityQueue_.end(), iterablePriorityQueue_.size());
  }
  template <class First, class Second>
  MessageBuilder& operator<<(const std::pair<First, Second>& pair_) {
    m_logger->stream() << ELPP_LITERAL("(");
    operator << (static_cast<First>(pair_.first));
    m_logger->stream() << ELPP_LITERAL(", ");
    operator << (static_cast<Second>(pair_.second));
    m_logger->stream() << ELPP_LITERAL(")");
    return *this;
  }
  template <std::size_t Size>
  MessageBuilder& operator<<(const std::bitset<Size>& bitset_) {
    m_logger->stream() << ELPP_LITERAL("[");
    operator << (bitset_.to_string());
    m_logger->stream() << ELPP_LITERAL("]");
    return *this;
  }
#  if defined(ELPP_LOG_STD_ARRAY)
  template <class T, std::size_t Size>
  inline MessageBuilder& operator<<(const std::array<T, Size>& array) {
    return writeIterator(array.begin(), array.end(), array.size());
  }
#  endif  // defined(ELPP_LOG_STD_ARRAY)
#  if defined(ELPP_LOG_UNORDERED_MAP)
  ELPP_ITERATOR_CONTAINER_LOG_FIVE_ARG(std::unordered_map)
  ELPP_ITERATOR_CONTAINER_LOG_FIVE_ARG(std::unordered_multimap)
#  endif  // defined(ELPP_LOG_UNORDERED_MAP)
#  if defined(ELPP_LOG_UNORDERED_SET)
  ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(std::unordered_set)
  ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(std::unordered_multiset)
#  endif  // defined(ELPP_LOG_UNORDERED_SET)
#endif  // defined(ELPP_STL_LOGGING)
#if defined(ELPP_QT_LOGGING)
  inline MessageBuilder& operator<<(const QString& msg) {
#  if defined(ELPP_UNICODE)
    m_logger->stream() << msg.toStdWString();
#  else
    m_logger->stream() << msg.toStdString();
#  endif  // defined(ELPP_UNICODE)
    return *this;
  }
  inline MessageBuilder& operator<<(const QByteArray& msg) {
    return operator << (QString(msg));
  }
  inline MessageBuilder& operator<<(const QStringRef& msg) {
    return operator<<(msg.toString());
  }
  inline MessageBuilder& operator<<(qint64 msg) {
#  if defined(ELPP_UNICODE)
    m_logger->stream() << QString::number(msg).toStdWString();
#  else
    m_logger->stream() << QString::number(msg).toStdString();
#  endif  // defined(ELPP_UNICODE)
    return *this;
  }
  inline MessageBuilder& operator<<(quint64 msg) {
#  if defined(ELPP_UNICODE)
    m_logger->stream() << QString::number(msg).toStdWString();
#  else
    m_logger->stream() << QString::number(msg).toStdString();
#  endif  // defined(ELPP_UNICODE)
    return *this;
  }
  inline MessageBuilder& operator<<(QChar msg) {
    m_logger->stream() << msg.toLatin1();
    return *this;
  }
  inline MessageBuilder& operator<<(const QLatin1String& msg) {
    m_logger->stream() << msg.latin1();
    return *this;
  }
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(QList)
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(QVector)
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(QQueue)
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(QSet)
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(QLinkedList)
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(QStack)
  template <typename First, typename Second>
  MessageBuilder& operator<<(const QPair<First, Second>& pair_) {
    m_logger->stream() << ELPP_LITERAL("(");
    operator << (static_cast<First>(pair_.first));
    m_logger->stream() << ELPP_LITERAL(", ");
    operator << (static_cast<Second>(pair_.second));
    m_logger->stream() << ELPP_LITERAL(")");
    return *this;
  }
  template <typename K, typename V>
  MessageBuilder& operator<<(const QMap<K, V>& map_) {
    m_logger->stream() << ELPP_LITERAL("[");
    QList<K> keys = map_.keys();
    typename QList<K>::const_iterator begin = keys.begin();
    typename QList<K>::const_iterator end = keys.end();
    int max_ = static_cast<int>(base::consts::kMaxLogPerContainer);  // to prevent warning
    for (int index_ = 0; begin != end && index_ < max_; ++index_, ++begin) {
      m_logger->stream() << ELPP_LITERAL("(");
      operator << (static_cast<K>(*begin));
      m_logger->stream() << ELPP_LITERAL(", ");
      operator << (static_cast<V>(map_.value(*begin)));
      m_logger->stream() << ELPP_LITERAL(")");
      m_logger->stream() << ((index_ < keys.size() -1) ? m_containerLogSeperator : ELPP_LITERAL(""));
    }
    if (begin != end) {
      m_logger->stream() << ELPP_LITERAL("...");
    }
    m_logger->stream() << ELPP_LITERAL("]");
    return *this;
  }
  template <typename K, typename V>
  inline MessageBuilder& operator<<(const QMultiMap<K, V>& map_) {
    operator << (static_cast<QMap<K, V>>(map_));
    return *this;
  }
  template <typename K, typename V>
  MessageBuilder& operator<<(const QHash<K, V>& hash_) {
    m_logger->stream() << ELPP_LITERAL("[");
    QList<K> keys = hash_.keys();
    typename QList<K>::const_iterator begin = keys.begin();
    typename QList<K>::const_iterator end = keys.end();
    int max_ = static_cast<int>(base::consts::kMaxLogPerContainer);  // prevent type warning
    for (int index_ = 0; begin != end && index_ < max_; ++index_, ++begin) {
      m_logger->stream() << ELPP_LITERAL("(");
      operator << (static_cast<K>(*begin));
      m_logger->stream() << ELPP_LITERAL(", ");
      operator << (static_cast<V>(hash_.value(*begin)));
      m_logger->stream() << ELPP_LITERAL(")");
      m_logger->stream() << ((index_ < keys.size() -1) ? m_containerLogSeperator : ELPP_LITERAL(""));
    }
    if (begin != end) {
      m_logger->stream() << ELPP_LITERAL("...");
    }
    m_logger->stream() << ELPP_LITERAL("]");
    return *this;
  }
  template <typename K, typename V>
  inline MessageBuilder& operator<<(const QMultiHash<K, V>& multiHash_) {
    operator << (static_cast<QHash<K, V>>(multiHash_));
    return *this;
  }
#endif  // defined(ELPP_QT_LOGGING)
#if defined(ELPP_BOOST_LOGGING)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(boost::container::vector)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(boost::container::stable_vector)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(boost::container::list)
  ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG(boost::container::deque)
  ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(boost::container::map)
  ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG(boost::container::flat_map)
  ELPP_ITERATOR_CONTAINER_LOG_THREE_ARG(boost::container::set)
  ELPP_ITERATOR_CONTAINER_LOG_THREE_ARG(boost::container::flat_set)
#endif  // defined(ELPP_BOOST_LOGGING)

  /// @brief Macro used internally that can be used externally to make containers easylogging++ friendly
  ///
  /// @detail This macro expands to write an ostream& operator<< for container. This container is expected to
  ///         have begin() and end() methods that return respective iterators
  /// @param ContainerType Type of container e.g, MyList from WX_DECLARE_LIST(int, MyList); in wxwidgets
  /// @param SizeMethod Method used to get size of container.
  /// @param ElementInstance Instance of element to be fed out. Insance name is "elem". See WXELPP_ENABLED macro
  ///        for an example usage
#define MAKE_CONTAINERELPP_FRIENDLY(ContainerType, SizeMethod, ElementInstance) \
el::base::type::ostream_t& operator<<(el::base::type::ostream_t& ss, const ContainerType& container) {\
const el::base::type::char_t* sep = ELPP->hasFlag(el::LoggingFlag::NewLineForContainer) ? \
ELPP_LITERAL("\n    ") : ELPP_LITERAL(", ");\
ContainerType::const_iterator elem = container.begin();\
ContainerType::const_iterator endElem = container.end();\
std::size_t size_ = container.SizeMethod; \
ss << ELPP_LITERAL("[");\
for (std::size_t i = 0; elem != endElem && i < el::base::consts::kMaxLogPerContainer; ++i, ++elem) { \
ss << ElementInstance;\
ss << ((i < size_ - 1) ? sep : ELPP_LITERAL(""));\
}\
if (elem != endElem) {\
ss << ELPP_LITERAL("...");\
}\
ss << ELPP_LITERAL("]");\
return ss;\
}
#if defined(ELPP_WXWIDGETS_LOGGING)
  ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG(wxVector)
#  define ELPP_WX_PTR_ENABLED(ContainerType) MAKE_CONTAINERELPP_FRIENDLY(ContainerType, size(), *(*elem))
#  define ELPP_WX_ENABLED(ContainerType) MAKE_CONTAINERELPP_FRIENDLY(ContainerType, size(), (*elem))
#  define ELPP_WX_HASH_MAP_ENABLED(ContainerType) MAKE_CONTAINERELPP_FRIENDLY(ContainerType, size(), \
ELPP_LITERAL("(") << elem->first << ELPP_LITERAL(", ") << elem->second << ELPP_LITERAL(")")
#else
#  define ELPP_WX_PTR_ENABLED(ContainerType)
#  define ELPP_WX_ENABLED(ContainerType)
#  define ELPP_WX_HASH_MAP_ENABLED(ContainerType)
#endif  // defined(ELPP_WXWIDGETS_LOGGING)
  // Other classes
  template <class Class>
  ELPP_SIMPLE_LOG(const Class&)
#undef ELPP_SIMPLE_LOG
#undef ELPP_ITERATOR_CONTAINER_LOG_ONE_ARG
#undef ELPP_ITERATOR_CONTAINER_LOG_TWO_ARG
#undef ELPP_ITERATOR_CONTAINER_LOG_THREE_ARG
#undef ELPP_ITERATOR_CONTAINER_LOG_FOUR_ARG
#undef ELPP_ITERATOR_CONTAINER_LOG_FIVE_ARG
 private:
  Logger* m_logger;
  const base::type::char_t* m_containerLogSeperator;

  template<class Iterator>
  MessageBuilder& writeIterator(Iterator begin_, Iterator end_, std::size_t size_) {
    m_logger->stream() << ELPP_LITERAL("[");
    for (std::size_t i = 0; begin_ != end_ && i < base::consts::kMaxLogPerContainer; ++i, ++begin_) {
      operator << (*begin_);
      m_logger->stream() << ((i < size_ - 1) ? m_containerLogSeperator : ELPP_LITERAL(""));
    }
    if (begin_ != end_) {
      m_logger->stream() << ELPP_LITERAL("...");
    }
    m_logger->stream() << ELPP_LITERAL("]");
    if (ELPP->hasFlag(LoggingFlag::AutoSpacing)) {
      m_logger->stream() << " ";
    }
    return *this;
  }
};
/// @brief Writes nothing - Used when certain log is disabled
class NullWriter : base::NoCopy {
 public:
  NullWriter(void) {}

  // Null manipulator
  inline NullWriter& operator<<(std::ostream& (*)(std::ostream&)) {
    return *this;
  }

  template <typename T>
  inline NullWriter& operator<<(const T&) {
    return *this;
  }

  inline operator bool() {
    return true;
  }
};
/// @brief Main entry point of each logging
class Writer : base::NoCopy {
 public:
  Writer(Level level, const char* file, base::type::LineNumber line,
         const char* func, base::DispatchAction dispatchAction = base::DispatchAction::NormalLog,
         base::type::VerboseLevel verboseLevel = 0) :
    m_msg(nullptr), m_level(level), m_file(file), m_line(line), m_func(func), m_verboseLevel(verboseLevel),
    m_logger(nullptr), m_proceed(false), m_dispatchAction(dispatchAction) {
  }

  Writer(LogMessage* msg, base::DispatchAction dispatchAction = base::DispatchAction::NormalLog) :
    m_msg(msg), m_level(msg != nullptr ? msg->level() : Level::Unknown),
    m_line(0), m_logger(nullptr), m_proceed(false), m_dispatchAction(dispatchAction) {
  }

  virtual ~Writer(void) {
    processDispatch();
  }

  template <typename T>
  inline Writer& operator<<(const T& log) {
#if ELPP_LOGGING_ENABLED
    if (m_proceed) {
      m_messageBuilder << log;
    }
#endif  // ELPP_LOGGING_ENABLED
    return *this;
  }

  inline Writer& operator<<(std::ostream& (*log)(std::ostream&)) {
#if ELPP_LOGGING_ENABLED
    if (m_proceed) {
      m_messageBuilder << log;
    }
#endif  // ELPP_LOGGING_ENABLED
    return *this;
  }

  inline operator bool() {
    return true;
  }

  Writer& construct(Logger* logger, bool needLock = true);
  Writer& construct(int count, const char* loggerIds, ...);
 protected:
  LogMessage* m_msg;
  Level m_level;
  const char* m_file;
  const base::type::LineNumber m_line;
  const char* m_func;
  base::type::VerboseLevel m_verboseLevel;
  Logger* m_logger;
  bool m_proceed;
  base::MessageBuilder m_messageBuilder;
  base::DispatchAction m_dispatchAction;
  std::vector<std::string> m_loggerIds;
  friend class el::Helpers;

  void initializeLogger(const std::string& loggerId, bool lookup = true, bool needLock = true);
  void processDispatch();
  void triggerDispatch(void);
};
class PErrorWriter : public base::Writer {
 public:
  PErrorWriter(Level level, const char* file, base::type::LineNumber line,
               const char* func, base::DispatchAction dispatchAction = base::DispatchAction::NormalLog,
               base::type::VerboseLevel verboseLevel = 0) :
    base::Writer(level, file, line, func, dispatchAction, verboseLevel) {
  }

  virtual ~PErrorWriter(void);
};
}  // namespace base
// Logging from Logger class. Why this is here? Because we have Storage and Writer class available
#if ELPP_VARIADIC_TEMPLATES_SUPPORTED
template <typename T, typename... Args>
void Logger::log_(Level level, int vlevel, const char* s, const T& value, const Args&... args) {
  base::MessageBuilder b;
  b.initialize(this);
  while (*s) {
    if (*s == base::consts::kFormatSpecifierChar) {
      if (*(s + 1) == base::consts::kFormatSpecifierChar) {
        ++s;
      } else {
        if (*(s + 1) == base::consts::kFormatSpecifierCharValue) {
          ++s;
          b << value;
          log_(level, vlevel, ++s, args...);
          return;
        }
      }
    }
    b << *s++;
  }
  ELPP_INTERNAL_ERROR("Too many arguments provided. Unable to handle. Please provide more format specifiers", false);
}
template <typename T>
void Logger::log_(Level level, int vlevel, const T& log) {
  if (level == Level::Verbose) {
    if (ELPP->vRegistry()->allowed(vlevel, __FILE__)) {
      base::Writer(Level::Verbose, "FILE", 0, "FUNCTION",
                   base::DispatchAction::NormalLog, vlevel).construct(this, false) << log;
    } else {
      stream().str(ELPP_LITERAL(""));
      releaseLock();
    }
  } else {
    base::Writer(level, "FILE", 0, "FUNCTION").construct(this, false) << log;
  }
}
template <typename T, typename... Args>
inline void Logger::log(Level level, const char* s, const T& value, const Args&... args) {
  acquireLock(); // released in Writer!
  log_(level, 0, s, value, args...);
}
template <typename T>
inline void Logger::log(Level level, const T& log) {
  acquireLock(); // released in Writer!
  log_(level, 0, log);
}
#  if ELPP_VERBOSE_LOG
template <typename T, typename... Args>
inline void Logger::verbose(int vlevel, const char* s, const T& value, const Args&... args) {
  acquireLock(); // released in Writer!
  log_(el::Level::Verbose, vlevel, s, value, args...);
}
template <typename T>
inline void Logger::verbose(int vlevel, const T& log) {
  acquireLock(); // released in Writer!
  log_(el::Level::Verbose, vlevel, log);
}
#  else
template <typename T, typename... Args>
inline void Logger::verbose(int, const char*, const T&, const Args&...) {
  return;
}
template <typename T>
inline void Logger::verbose(int, const T&) {
  return;
}
#  endif  // ELPP_VERBOSE_LOG
#  define LOGGER_LEVEL_WRITERS(FUNCTION_NAME, LOG_LEVEL)\
template <typename T, typename... Args>\
inline void Logger::FUNCTION_NAME(const char* s, const T& value, const Args&... args) {\
log(LOG_LEVEL, s, value, args...);\
}\
template <typename T>\
inline void Logger::FUNCTION_NAME(const T& value) {\
log(LOG_LEVEL, value);\
}
#  define LOGGER_LEVEL_WRITERS_DISABLED(FUNCTION_NAME, LOG_LEVEL)\
template <typename T, typename... Args>\
inline void Logger::FUNCTION_NAME(const char*, const T&, const Args&...) {\
return;\
}\
template <typename T>\
inline void Logger::FUNCTION_NAME(const T&) {\
return;\
}

#  if ELPP_INFO_LOG
LOGGER_LEVEL_WRITERS(info, Level::Info)
#  else
LOGGER_LEVEL_WRITERS_DISABLED(info, Level::Info)
#  endif // ELPP_INFO_LOG
#  if ELPP_DEBUG_LOG
LOGGER_LEVEL_WRITERS(debug, Level::Debug)
#  else
LOGGER_LEVEL_WRITERS_DISABLED(debug, Level::Debug)
#  endif // ELPP_DEBUG_LOG
#  if ELPP_WARNING_LOG
LOGGER_LEVEL_WRITERS(warn, Level::Warning)
#  else
LOGGER_LEVEL_WRITERS_DISABLED(warn, Level::Warning)
#  endif // ELPP_WARNING_LOG
#  if ELPP_ERROR_LOG
LOGGER_LEVEL_WRITERS(error, Level::Error)
#  else
LOGGER_LEVEL_WRITERS_DISABLED(error, Level::Error)
#  endif // ELPP_ERROR_LOG
#  if ELPP_FATAL_LOG
LOGGER_LEVEL_WRITERS(fatal, Level::Fatal)
#  else
LOGGER_LEVEL_WRITERS_DISABLED(fatal, Level::Fatal)
#  endif // ELPP_FATAL_LOG
#  if ELPP_TRACE_LOG
LOGGER_LEVEL_WRITERS(trace, Level::Trace)
#  else
LOGGER_LEVEL_WRITERS_DISABLED(trace, Level::Trace)
#  endif // ELPP_TRACE_LOG
#  undef LOGGER_LEVEL_WRITERS
#  undef LOGGER_LEVEL_WRITERS_DISABLED
#endif // ELPP_VARIADIC_TEMPLATES_SUPPORTED
#if ELPP_COMPILER_MSVC
#  define ELPP_VARIADIC_FUNC_MSVC(variadicFunction, variadicArgs) variadicFunction variadicArgs
#  define ELPP_VARIADIC_FUNC_MSVC_RUN(variadicFunction, ...) ELPP_VARIADIC_FUNC_MSVC(variadicFunction, (__VA_ARGS__))
#  define el_getVALength(...) ELPP_VARIADIC_FUNC_MSVC_RUN(el_resolveVALength, 0, ## __VA_ARGS__,\
10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#else
#  if ELPP_COMPILER_CLANG
#    define el_getVALength(...) el_resolveVALength(0, __VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#  else
#    define el_getVALength(...) el_resolveVALength(0, ## __VA_ARGS__, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#  endif // ELPP_COMPILER_CLANG
#endif // ELPP_COMPILER_MSVC
#define el_resolveVALength(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, N, ...) N
#define ELPP_WRITE_LOG(writer, level, dispatchAction, ...) \
writer(level, __FILE__, __LINE__, ELPP_FUNC, dispatchAction).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#define ELPP_WRITE_LOG_IF(writer, condition, level, dispatchAction, ...) if (condition) \
writer(level, __FILE__, __LINE__, ELPP_FUNC, dispatchAction).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#define ELPP_WRITE_LOG_EVERY_N(writer, occasion, level, dispatchAction, ...) \
ELPP->validateEveryNCounter(__FILE__, __LINE__, occasion) && \
writer(level, __FILE__, __LINE__, ELPP_FUNC, dispatchAction).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#define ELPP_WRITE_LOG_AFTER_N(writer, n, level, dispatchAction, ...) \
ELPP->validateAfterNCounter(__FILE__, __LINE__, n) && \
writer(level, __FILE__, __LINE__, ELPP_FUNC, dispatchAction).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#define ELPP_WRITE_LOG_N_TIMES(writer, n, level, dispatchAction, ...) \
ELPP->validateNTimesCounter(__FILE__, __LINE__, n) && \
writer(level, __FILE__, __LINE__, ELPP_FUNC, dispatchAction).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
class PerformanceTrackingData {
 public:
  enum class DataType : base::type::EnumType {
    Checkpoint = 1, Complete = 2
  };
  // Do not use constructor, will run into multiple definition error, use init(PerformanceTracker*)
  explicit PerformanceTrackingData(DataType dataType) : m_performanceTracker(nullptr),
    m_dataType(dataType), m_firstCheckpoint(false), m_file(""), m_line(0), m_func("") {}
  inline const std::string* blockName(void) const;
  inline const struct timeval* startTime(void) const;
  inline const struct timeval* endTime(void) const;
  inline const struct timeval* lastCheckpointTime(void) const;
  inline const base::PerformanceTracker* performanceTracker(void) const {
    return m_performanceTracker;
  }
  inline PerformanceTrackingData::DataType dataType(void) const {
    return m_dataType;
  }
  inline bool firstCheckpoint(void) const {
    return m_firstCheckpoint;
  }
  inline std::string checkpointId(void) const {
    return m_checkpointId;
  }
  inline const char* file(void) const {
    return m_file;
  }
  inline base::type::LineNumber line(void) const {
    return m_line;
  }
  inline const char* func(void) const {
    return m_func;
  }
  inline const base::type::string_t* formattedTimeTaken() const {
    return &m_formattedTimeTaken;
  }
  inline const std::string& loggerId(void) const;
 private:
  base::PerformanceTracker* m_performanceTracker;
  base::type::string_t m_formattedTimeTaken;
  PerformanceTrackingData::DataType m_dataType;
  bool m_firstCheckpoint;
  std::string m_checkpointId;
  const char* m_file;
  base::type::LineNumber m_line;
  const char* m_func;
  inline void init(base::PerformanceTracker* performanceTracker, bool firstCheckpoint = false) {
    m_performanceTracker = performanceTracker;
    m_firstCheckpoint = firstCheckpoint;
  }

  friend class el::base::PerformanceTracker;
};
namespace base {
/// @brief Represents performanceTracker block of code that conditionally adds performance status to log
///        either when goes outside the scope of when checkpoint() is called
class PerformanceTracker : public base::threading::ThreadSafe, public Loggable {
 public:
  PerformanceTracker(const std::string& blockName,
                     base::TimestampUnit timestampUnit = base::TimestampUnit::Millisecond,
                     const std::string& loggerId = std::string(el::base::consts::kPerformanceLoggerId),
                     bool scopedLog = true, Level level = base::consts::kPerformanceTrackerDefaultLevel);
  /// @brief Copy constructor
  PerformanceTracker(const PerformanceTracker& t) :
    m_blockName(t.m_blockName), m_timestampUnit(t.m_timestampUnit), m_loggerId(t.m_loggerId), m_scopedLog(t.m_scopedLog),
    m_level(t.m_level), m_hasChecked(t.m_hasChecked), m_lastCheckpointId(t.m_lastCheckpointId), m_enabled(t.m_enabled),
    m_startTime(t.m_startTime), m_endTime(t.m_endTime), m_lastCheckpointTime(t.m_lastCheckpointTime) {
  }
  virtual ~PerformanceTracker(void);
  /// @brief A checkpoint for current performanceTracker block.
  void checkpoint(const std::string& id = std::string(), const char* file = __FILE__,
                  base::type::LineNumber line = __LINE__,
                  const char* func = "");
  inline Level level(void) const {
    return m_level;
  }
 private:
  std::string m_blockName;
  base::TimestampUnit m_timestampUnit;
  std::string m_loggerId;
  bool m_scopedLog;
  Level m_level;
  bool m_hasChecked;
  std::string m_lastCheckpointId;
  bool m_enabled;
  struct timeval m_startTime, m_endTime, m_lastCheckpointTime;

  PerformanceTracker(void);

  friend class el::PerformanceTrackingData;
  friend class base::DefaultPerformanceTrackingCallback;

  const inline base::type::string_t getFormattedTimeTaken() const {
    return getFormattedTimeTaken(m_startTime);
  }

  const base::type::string_t getFormattedTimeTaken(struct timeval startTime) const;

  virtual inline void log(el::base::type::ostream_t& os) const {
    os << getFormattedTimeTaken();
  }
};
class DefaultPerformanceTrackingCallback : public PerformanceTrackingCallback {
 protected:
  void handle(const PerformanceTrackingData* data) {
    m_data = data;
    base::type::stringstream_t ss;
    if (m_data->dataType() == PerformanceTrackingData::DataType::Complete) {
      ss << ELPP_LITERAL("Executed [") << m_data->blockName()->c_str() << ELPP_LITERAL("] in [") <<
         *m_data->formattedTimeTaken() << ELPP_LITERAL("]");
    } else {
      ss << ELPP_LITERAL("Performance checkpoint");
      if (!m_data->checkpointId().empty()) {
        ss << ELPP_LITERAL(" [") << m_data->checkpointId().c_str() << ELPP_LITERAL("]");
      }
      ss << ELPP_LITERAL(" for block [") << m_data->blockName()->c_str() << ELPP_LITERAL("] : [") <<
         *m_data->performanceTracker();
      if (!ELPP->hasFlag(LoggingFlag::DisablePerformanceTrackingCheckpointComparison)
          && m_data->performanceTracker()->m_hasChecked) {
        ss << ELPP_LITERAL(" ([") << *m_data->formattedTimeTaken() << ELPP_LITERAL("] from ");
        if (m_data->performanceTracker()->m_lastCheckpointId.empty()) {
          ss << ELPP_LITERAL("last checkpoint");
        } else {
          ss << ELPP_LITERAL("checkpoint '") << m_data->performanceTracker()->m_lastCheckpointId.c_str() << ELPP_LITERAL("'");
        }
        ss << ELPP_LITERAL(")]");
      } else {
        ss << ELPP_LITERAL("]");
      }
    }
    el::base::Writer(m_data->performanceTracker()->level(), m_data->file(), m_data->line(), m_data->func()).construct(1,
        m_data->loggerId().c_str()) << ss.str();
  }
 private:
  const PerformanceTrackingData* m_data;
};
}  // namespace base
inline const std::string* PerformanceTrackingData::blockName() const {
  return const_cast<const std::string*>(&m_performanceTracker->m_blockName);
}
inline const struct timeval* PerformanceTrackingData::startTime() const {
  return const_cast<const struct timeval*>(&m_performanceTracker->m_startTime);
}
inline const struct timeval* PerformanceTrackingData::endTime() const {
  return const_cast<const struct timeval*>(&m_performanceTracker->m_endTime);
}
inline const struct timeval* PerformanceTrackingData::lastCheckpointTime() const {
  return const_cast<const struct timeval*>(&m_performanceTracker->m_lastCheckpointTime);
}
inline const std::string& PerformanceTrackingData::loggerId(void) const {
  return m_performanceTracker->m_loggerId;
}
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
namespace base {
/// @brief Contains some internal debugging tools like crash handler and stack tracer
namespace debug {
#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)
class StackTrace : base::NoCopy {
 public:
  static const unsigned int kMaxStack = 64;
  static const unsigned int kStackStart = 2;  // We want to skip c'tor and StackTrace::generateNew()
  class StackTraceEntry {
   public:
    StackTraceEntry(std::size_t index, const std::string& loc, const std::string& demang, const std::string& hex,
                    const std::string& addr);
    StackTraceEntry(std::size_t index, const std::string& loc) :
      m_index(index),
      m_location(loc) {
    }
    std::size_t m_index;
    std::string m_location;
    std::string m_demangled;
    std::string m_hex;
    std::string m_addr;
    friend std::ostream& operator<<(std::ostream& ss, const StackTraceEntry& si);

   private:
    StackTraceEntry(void);
  };

  StackTrace(void) {
    generateNew();
  }

  virtual ~StackTrace(void) {
  }

  inline std::vector<StackTraceEntry>& getLatestStack(void) {
    return m_stack;
  }

  friend std::ostream& operator<<(std::ostream& os, const StackTrace& st);

 private:
  std::vector<StackTraceEntry> m_stack;

  void generateNew(void);
};
/// @brief Handles unexpected crashes
class CrashHandler : base::NoCopy {
 public:
  typedef void (*Handler)(int);

  explicit CrashHandler(bool useDefault);
  explicit CrashHandler(const Handler& cHandler) {
    setHandler(cHandler);
  }
  void setHandler(const Handler& cHandler);

 private:
  Handler m_handler;
};
#else
class CrashHandler {
 public:
  explicit CrashHandler(bool) {}
};
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)
}  // namespace debug
}  // namespace base
extern base::debug::CrashHandler elCrashHandler;
#define MAKE_LOGGABLE(ClassType, ClassInstance, OutputStreamInstance) \
el::base::type::ostream_t& operator<<(el::base::type::ostream_t& OutputStreamInstance, const ClassType& ClassInstance)
/// @brief Initializes syslog with process ID, options and facility. calls closelog() on d'tor
class SysLogInitializer {
 public:
  SysLogInitializer(const char* processIdent, int options = 0, int facility = 0) {
#if defined(ELPP_SYSLOG)
    openlog(processIdent, options, facility);
#else
    ELPP_UNUSED(processIdent);
    ELPP_UNUSED(options);
    ELPP_UNUSED(facility);
#endif  // defined(ELPP_SYSLOG)
  }
  virtual ~SysLogInitializer(void) {
#if defined(ELPP_SYSLOG)
    closelog();
#endif  // defined(ELPP_SYSLOG)
  }
};
#define ELPP_INITIALIZE_SYSLOG(id, opt, fac) el::SysLogInitializer elSyslogInit(id, opt, fac)
/// @brief Static helpers for developers
class Helpers : base::StaticClass {
 public:
  /// @brief Shares logging repository (base::Storage)
  static inline void setStorage(base::type::StoragePointer storage) {
    ELPP = storage;
  }
  /// @return Main storage repository
  static inline base::type::StoragePointer storage() {
    return ELPP;
  }
  /// @brief Sets application arguments and figures out whats active for logging and whats not.
  static inline void setArgs(int argc, char** argv) {
    ELPP->setApplicationArguments(argc, argv);
  }
  /// @copydoc setArgs(int argc, char** argv)
  static inline void setArgs(int argc, const char** argv) {
    ELPP->setApplicationArguments(argc, const_cast<char**>(argv));
  }
  /// @brief Sets thread name for current thread. Requires std::thread
  static inline void setThreadName(const std::string& name) {
    ELPP->setThreadName(name);
  }
  static inline std::string getThreadName() {
    return ELPP->getThreadName(base::threading::getCurrentThreadId());
  }
#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)
  /// @brief Overrides default crash handler and installs custom handler.
  /// @param crashHandler A functor with no return type that takes single int argument.
  ///        Handler is a typedef with specification: void (*Handler)(int)
  static inline void setCrashHandler(const el::base::debug::CrashHandler::Handler& crashHandler) {
    el::elCrashHandler.setHandler(crashHandler);
  }
  /// @brief Abort due to crash with signal in parameter
  /// @param sig Crash signal
  static void crashAbort(int sig, const char* sourceFile = "", unsigned int long line = 0);
  /// @brief Logs reason of crash as per sig
  /// @param sig Crash signal
  /// @param stackTraceIfAvailable Includes stack trace if available
  /// @param level Logging level
  /// @param logger Logger to use for logging
  static void logCrashReason(int sig, bool stackTraceIfAvailable = false,
                             Level level = Level::Fatal, const char* logger = base::consts::kDefaultLoggerId);
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)
  /// @brief Installs pre rollout callback, this callback is triggered when log file is about to be rolled out
  ///        (can be useful for backing up)
  static inline void installPreRollOutCallback(const PreRollOutCallback& callback) {
    ELPP->setPreRollOutCallback(callback);
  }
  /// @brief Uninstalls pre rollout callback
  static inline void uninstallPreRollOutCallback(void) {
    ELPP->unsetPreRollOutCallback();
  }
  /// @brief Installs post log dispatch callback, this callback is triggered when log is dispatched
  template <typename T>
  static inline bool installLogDispatchCallback(const std::string& id) {
    return ELPP->installLogDispatchCallback<T>(id);
  }
  /// @brief Uninstalls log dispatch callback
  template <typename T>
  static inline void uninstallLogDispatchCallback(const std::string& id) {
    ELPP->uninstallLogDispatchCallback<T>(id);
  }
  template <typename T>
  static inline T* logDispatchCallback(const std::string& id) {
    return ELPP->logDispatchCallback<T>(id);
  }
#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
  /// @brief Installs post performance tracking callback, this callback is triggered when performance tracking is finished
  template <typename T>
  static inline bool installPerformanceTrackingCallback(const std::string& id) {
    return ELPP->installPerformanceTrackingCallback<T>(id);
  }
  /// @brief Uninstalls post performance tracking handler
  template <typename T>
  static inline void uninstallPerformanceTrackingCallback(const std::string& id) {
    ELPP->uninstallPerformanceTrackingCallback<T>(id);
  }
  template <typename T>
  static inline T* performanceTrackingCallback(const std::string& id) {
    return ELPP->performanceTrackingCallback<T>(id);
  }
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
  /// @brief Converts template to std::string - useful for loggable classes to log containers within log(std::ostream&) const
  template <typename T>
  static std::string convertTemplateToStdString(const T& templ) {
    el::Logger* logger =
      ELPP->registeredLoggers()->get(el::base::consts::kDefaultLoggerId);
    if (logger == nullptr) {
      return std::string();
    }
    base::MessageBuilder b;
    b.initialize(logger);
    logger->acquireLock();
    b << templ;
#if defined(ELPP_UNICODE)
    std::string s = std::string(logger->stream().str().begin(), logger->stream().str().end());
#else
    std::string s = logger->stream().str();
#endif  // defined(ELPP_UNICODE)
    logger->stream().str(ELPP_LITERAL(""));
    logger->releaseLock();
    return s;
  }
  /// @brief Returns command line arguments (pointer) provided to easylogging++
  static inline const el::base::utils::CommandLineArgs* commandLineArgs(void) {
    return ELPP->commandLineArgs();
  }
  /// @brief Reserve space for custom format specifiers for performance
  /// @see std::vector::reserve
  static inline void reserveCustomFormatSpecifiers(std::size_t size) {
    ELPP->m_customFormatSpecifiers.reserve(size);
  }
  /// @brief Installs user defined format specifier and handler
  static inline void installCustomFormatSpecifier(const CustomFormatSpecifier& customFormatSpecifier) {
    ELPP->installCustomFormatSpecifier(customFormatSpecifier);
  }
  /// @brief Uninstalls user defined format specifier and handler
  static inline bool uninstallCustomFormatSpecifier(const char* formatSpecifier) {
    return ELPP->uninstallCustomFormatSpecifier(formatSpecifier);
  }
  /// @brief Returns true if custom format specifier is installed
  static inline bool hasCustomFormatSpecifier(const char* formatSpecifier) {
    return ELPP->hasCustomFormatSpecifier(formatSpecifier);
  }
  static inline void validateFileRolling(Logger* logger, Level level) {
    if (ELPP == nullptr || logger == nullptr) return;
    logger->m_typedConfigurations->validateFileRolling(level, ELPP->preRollOutCallback());
  }
};
/// @brief Static helpers to deal with loggers and their configurations
class Loggers : base::StaticClass {
 public:
  /// @brief Gets existing or registers new logger
  static Logger* getLogger(const std::string& identity, bool registerIfNotAvailable = true);
  /// @brief Changes default log builder for future loggers
  static void setDefaultLogBuilder(el::LogBuilderPtr& logBuilderPtr);
  /// @brief Installs logger registration callback, this callback is triggered when new logger is registered
  template <typename T>
  static inline bool installLoggerRegistrationCallback(const std::string& id) {
    return ELPP->registeredLoggers()->installLoggerRegistrationCallback<T>(id);
  }
  /// @brief Uninstalls log dispatch callback
  template <typename T>
  static inline void uninstallLoggerRegistrationCallback(const std::string& id) {
    ELPP->registeredLoggers()->uninstallLoggerRegistrationCallback<T>(id);
  }
  template <typename T>
  static inline T* loggerRegistrationCallback(const std::string& id) {
    return ELPP->registeredLoggers()->loggerRegistrationCallback<T>(id);
  }
  /// @brief Unregisters logger - use it only when you know what you are doing, you may unregister
  ///        loggers initialized / used by third-party libs.
  static bool unregisterLogger(const std::string& identity);
  /// @brief Whether or not logger with id is registered
  static bool hasLogger(const std::string& identity);
  /// @brief Reconfigures specified logger with new configurations
  static Logger* reconfigureLogger(Logger* logger, const Configurations& configurations);
  /// @brief Reconfigures logger with new configurations after looking it up using identity
  static Logger* reconfigureLogger(const std::string& identity, const Configurations& configurations);
  /// @brief Reconfigures logger's single configuration
  static Logger* reconfigureLogger(const std::string& identity, ConfigurationType configurationType,
                                   const std::string& value);
  /// @brief Reconfigures all the existing loggers with new configurations
  static void reconfigureAllLoggers(const Configurations& configurations);
  /// @brief Reconfigures single configuration for all the loggers
  static inline void reconfigureAllLoggers(ConfigurationType configurationType, const std::string& value) {
    reconfigureAllLoggers(Level::Global, configurationType, value);
  }
  /// @brief Reconfigures single configuration for all the loggers for specified level
  static void reconfigureAllLoggers(Level level, ConfigurationType configurationType,
                                    const std::string& value);
  /// @brief Sets default configurations. This configuration is used for future (and conditionally for existing) loggers
  static void setDefaultConfigurations(const Configurations& configurations,
                                       bool reconfigureExistingLoggers = false);
  /// @brief Returns current default
  static const Configurations* defaultConfigurations(void);
  /// @brief Returns log stream reference pointer if needed by user
  static const base::LogStreamsReferenceMap* logStreamsReference(void);
  /// @brief Default typed configuration based on existing defaultConf
  static base::TypedConfigurations defaultTypedConfigurations(void);
  /// @brief Populates all logger IDs in current repository.
  /// @param [out] targetList List of fill up.
  static std::vector<std::string>* populateAllLoggerIds(std::vector<std::string>* targetList);
  /// @brief Sets configurations from global configuration file.
  static void configureFromGlobal(const char* globalConfigurationFilePath);
  /// @brief Configures loggers using command line arg. Ensure you have already set command line args,
  /// @return False if invalid argument or argument with no value provided, true if attempted to configure logger.
  ///         If true is returned that does not mean it has been configured successfully, it only means that it
  ///         has attempeted to configure logger using configuration file provided in argument
  static bool configureFromArg(const char* argKey);
  /// @brief Flushes all loggers for all levels - Be careful if you dont know how many loggers are registered
  static void flushAll(void);
  /// @brief Adds logging flag used internally.
  static inline void addFlag(LoggingFlag flag) {
    ELPP->addFlag(flag);
  }
  /// @brief Removes logging flag used internally.
  static inline void removeFlag(LoggingFlag flag) {
    ELPP->removeFlag(flag);
  }
  /// @brief Determines whether or not certain flag is active
  static inline bool hasFlag(LoggingFlag flag) {
    return ELPP->hasFlag(flag);
  }
  /// @brief Adds flag and removes it when scope goes out
  class ScopedAddFlag {
   public:
    ScopedAddFlag(LoggingFlag flag) : m_flag(flag) {
      Loggers::addFlag(m_flag);
    }
    ~ScopedAddFlag(void) {
      Loggers::removeFlag(m_flag);
    }
   private:
    LoggingFlag m_flag;
  };
  /// @brief Removes flag and add it when scope goes out
  class ScopedRemoveFlag {
   public:
    ScopedRemoveFlag(LoggingFlag flag) : m_flag(flag) {
      Loggers::removeFlag(m_flag);
    }
    ~ScopedRemoveFlag(void) {
      Loggers::addFlag(m_flag);
    }
   private:
    LoggingFlag m_flag;
  };
  /// @brief Sets hierarchy for logging. Needs to enable logging flag (HierarchicalLogging)
  static void setLoggingLevel(Level level) {
    ELPP->setLoggingLevel(level);
  }
  /// @brief Sets verbose level on the fly
  static void setVerboseLevel(base::type::VerboseLevel level);
  /// @brief Gets current verbose level
  static base::type::VerboseLevel verboseLevel(void);
  /// @brief Sets vmodules as specified (on the fly)
  static void setVModules(const char* modules);
  /// @brief Clears vmodules
  static void clearVModules(void);
};
class VersionInfo : base::StaticClass {
 public:
  /// @brief Current version number
  static const std::string version(void);

  /// @brief Release date of current version
  static const std::string releaseDate(void);
};
}  // namespace el
#undef VLOG_IS_ON
/// @brief Determines whether verbose logging is on for specified level current file.
#define VLOG_IS_ON(verboseLevel) (ELPP->vRegistry()->allowed(verboseLevel, __FILE__))
#undef TIMED_BLOCK
#undef TIMED_SCOPE
#undef TIMED_SCOPE_IF
#undef TIMED_FUNC
#undef TIMED_FUNC_IF
#undef ELPP_MIN_UNIT
#if defined(ELPP_PERFORMANCE_MICROSECONDS)
#  define ELPP_MIN_UNIT el::base::TimestampUnit::Microsecond
#else
#  define ELPP_MIN_UNIT el::base::TimestampUnit::Millisecond
#endif  // (defined(ELPP_PERFORMANCE_MICROSECONDS))
/// @brief Performance tracked scope. Performance gets written when goes out of scope using
///        'performance' logger.
///
/// @detail Please note in order to check the performance at a certain time you can use obj->checkpoint();
/// @see el::base::PerformanceTracker
/// @see el::base::PerformanceTracker::checkpoint
// Note: Do not surround this definition with null macro because of obj instance
#define TIMED_SCOPE_IF(obj, blockname, condition) el::base::type::PerformanceTrackerPtr obj( condition ? \
  new el::base::PerformanceTracker(blockname, ELPP_MIN_UNIT) : nullptr )
#define TIMED_SCOPE(obj, blockname) TIMED_SCOPE_IF(obj, blockname, true)
#define TIMED_BLOCK(obj, blockName) for (struct { int i; el::base::type::PerformanceTrackerPtr timer; } obj = { 0, \
  el::base::type::PerformanceTrackerPtr(new el::base::PerformanceTracker(blockName, ELPP_MIN_UNIT)) }; obj.i < 1; ++obj.i)
/// @brief Performance tracked function. Performance gets written when goes out of scope using
///        'performance' logger.
///
/// @detail Please note in order to check the performance at a certain time you can use obj->checkpoint();
/// @see el::base::PerformanceTracker
/// @see el::base::PerformanceTracker::checkpoint
#define TIMED_FUNC_IF(obj,condition) TIMED_SCOPE_IF(obj, ELPP_FUNC, condition)
#define TIMED_FUNC(obj) TIMED_SCOPE(obj, ELPP_FUNC)
#undef PERFORMANCE_CHECKPOINT
#undef PERFORMANCE_CHECKPOINT_WITH_ID
#define PERFORMANCE_CHECKPOINT(obj) obj->checkpoint(std::string(), __FILE__, __LINE__, ELPP_FUNC)
#define PERFORMANCE_CHECKPOINT_WITH_ID(obj, id) obj->checkpoint(id, __FILE__, __LINE__, ELPP_FUNC)
#undef ELPP_COUNTER
#undef ELPP_COUNTER_POS
/// @brief Gets hit counter for file/line
#define ELPP_COUNTER (ELPP->hitCounters()->getCounter(__FILE__, __LINE__))
/// @brief Gets hit counter position for file/line, -1 if not registered yet
#define ELPP_COUNTER_POS (ELPP_COUNTER == nullptr ? -1 : ELPP_COUNTER->hitCounts())
// Undef levels to support LOG(LEVEL)
#undef INFO
#undef WARNING
#undef DEBUG
#undef ERROR
#undef FATAL
#undef TRACE
#undef VERBOSE
// Undef existing
#undef CINFO
#undef CWARNING
#undef CDEBUG
#undef CFATAL
#undef CERROR
#undef CTRACE
#undef CVERBOSE
#undef CINFO_IF
#undef CWARNING_IF
#undef CDEBUG_IF
#undef CERROR_IF
#undef CFATAL_IF
#undef CTRACE_IF
#undef CVERBOSE_IF
#undef CINFO_EVERY_N
#undef CWARNING_EVERY_N
#undef CDEBUG_EVERY_N
#undef CERROR_EVERY_N
#undef CFATAL_EVERY_N
#undef CTRACE_EVERY_N
#undef CVERBOSE_EVERY_N
#undef CINFO_AFTER_N
#undef CWARNING_AFTER_N
#undef CDEBUG_AFTER_N
#undef CERROR_AFTER_N
#undef CFATAL_AFTER_N
#undef CTRACE_AFTER_N
#undef CVERBOSE_AFTER_N
#undef CINFO_N_TIMES
#undef CWARNING_N_TIMES
#undef CDEBUG_N_TIMES
#undef CERROR_N_TIMES
#undef CFATAL_N_TIMES
#undef CTRACE_N_TIMES
#undef CVERBOSE_N_TIMES
// Normal logs
#if ELPP_INFO_LOG
#  define CINFO(writer, dispatchAction, ...) ELPP_WRITE_LOG(writer, el::Level::Info, dispatchAction, __VA_ARGS__)
#else
#  define CINFO(writer, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_INFO_LOG
#if ELPP_WARNING_LOG
#  define CWARNING(writer, dispatchAction, ...) ELPP_WRITE_LOG(writer, el::Level::Warning, dispatchAction, __VA_ARGS__)
#else
#  define CWARNING(writer, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_WARNING_LOG
#if ELPP_DEBUG_LOG
#  define CDEBUG(writer, dispatchAction, ...) ELPP_WRITE_LOG(writer, el::Level::Debug, dispatchAction, __VA_ARGS__)
#else
#  define CDEBUG(writer, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_DEBUG_LOG
#if ELPP_ERROR_LOG
#  define CERROR(writer, dispatchAction, ...) ELPP_WRITE_LOG(writer, el::Level::Error, dispatchAction, __VA_ARGS__)
#else
#  define CERROR(writer, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_ERROR_LOG
#if ELPP_FATAL_LOG
#  define CFATAL(writer, dispatchAction, ...) ELPP_WRITE_LOG(writer, el::Level::Fatal, dispatchAction, __VA_ARGS__)
#else
#  define CFATAL(writer, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_FATAL_LOG
#if ELPP_TRACE_LOG
#  define CTRACE(writer, dispatchAction, ...) ELPP_WRITE_LOG(writer, el::Level::Trace, dispatchAction, __VA_ARGS__)
#else
#  define CTRACE(writer, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_TRACE_LOG
#if ELPP_VERBOSE_LOG
#  define CVERBOSE(writer, vlevel, dispatchAction, ...) if (VLOG_IS_ON(vlevel)) writer(\
el::Level::Verbose, __FILE__, __LINE__, ELPP_FUNC, dispatchAction, vlevel).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#else
#  define CVERBOSE(writer, vlevel, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_VERBOSE_LOG
// Conditional logs
#if ELPP_INFO_LOG
#  define CINFO_IF(writer, condition_, dispatchAction, ...) \
ELPP_WRITE_LOG_IF(writer, (condition_), el::Level::Info, dispatchAction, __VA_ARGS__)
#else
#  define CINFO_IF(writer, condition_, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_INFO_LOG
#if ELPP_WARNING_LOG
#  define CWARNING_IF(writer, condition_, dispatchAction, ...)\
ELPP_WRITE_LOG_IF(writer, (condition_), el::Level::Warning, dispatchAction, __VA_ARGS__)
#else
#  define CWARNING_IF(writer, condition_, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_WARNING_LOG
#if ELPP_DEBUG_LOG
#  define CDEBUG_IF(writer, condition_, dispatchAction, ...)\
ELPP_WRITE_LOG_IF(writer, (condition_), el::Level::Debug, dispatchAction, __VA_ARGS__)
#else
#  define CDEBUG_IF(writer, condition_, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_DEBUG_LOG
#if ELPP_ERROR_LOG
#  define CERROR_IF(writer, condition_, dispatchAction, ...)\
ELPP_WRITE_LOG_IF(writer, (condition_), el::Level::Error, dispatchAction, __VA_ARGS__)
#else
#  define CERROR_IF(writer, condition_, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_ERROR_LOG
#if ELPP_FATAL_LOG
#  define CFATAL_IF(writer, condition_, dispatchAction, ...)\
ELPP_WRITE_LOG_IF(writer, (condition_), el::Level::Fatal, dispatchAction, __VA_ARGS__)
#else
#  define CFATAL_IF(writer, condition_, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_FATAL_LOG
#if ELPP_TRACE_LOG
#  define CTRACE_IF(writer, condition_, dispatchAction, ...)\
ELPP_WRITE_LOG_IF(writer, (condition_), el::Level::Trace, dispatchAction, __VA_ARGS__)
#else
#  define CTRACE_IF(writer, condition_, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_TRACE_LOG
#if ELPP_VERBOSE_LOG
#  define CVERBOSE_IF(writer, condition_, vlevel, dispatchAction, ...) if (VLOG_IS_ON(vlevel) && (condition_)) writer( \
el::Level::Verbose, __FILE__, __LINE__, ELPP_FUNC, dispatchAction, vlevel).construct(el_getVALength(__VA_ARGS__), __VA_ARGS__)
#else
#  define CVERBOSE_IF(writer, condition_, vlevel, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_VERBOSE_LOG
// Occasional logs
#if ELPP_INFO_LOG
#  define CINFO_EVERY_N(writer, occasion, dispatchAction, ...)\
ELPP_WRITE_LOG_EVERY_N(writer, occasion, el::Level::Info, dispatchAction, __VA_ARGS__)
#else
#  define CINFO_EVERY_N(writer, occasion, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_INFO_LOG
#if ELPP_WARNING_LOG
#  define CWARNING_EVERY_N(writer, occasion, dispatchAction, ...)\
ELPP_WRITE_LOG_EVERY_N(writer, occasion, el::Level::Warning, dispatchAction, __VA_ARGS__)
#else
#  define CWARNING_EVERY_N(writer, occasion, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_WARNING_LOG
#if ELPP_DEBUG_LOG
#  define CDEBUG_EVERY_N(writer, occasion, dispatchAction, ...)\
ELPP_WRITE_LOG_EVERY_N(writer, occasion, el::Level::Debug, dispatchAction, __VA_ARGS__)
#else
#  define CDEBUG_EVERY_N(writer, occasion, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_DEBUG_LOG
#if ELPP_ERROR_LOG
#  define CERROR_EVERY_N(writer, occasion, dispatchAction, ...)\
ELPP_WRITE_LOG_EVERY_N(writer, occasion, el::Level::Error, dispatchAction, __VA_ARGS__)
#else
#  define CERROR_EVERY_N(writer, occasion, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_ERROR_LOG
#if ELPP_FATAL_LOG
#  define CFATAL_EVERY_N(writer, occasion, dispatchAction, ...)\
ELPP_WRITE_LOG_EVERY_N(writer, occasion, el::Level::Fatal, dispatchAction, __VA_ARGS__)
#else
#  define CFATAL_EVERY_N(writer, occasion, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_FATAL_LOG
#if ELPP_TRACE_LOG
#  define CTRACE_EVERY_N(writer, occasion, dispatchAction, ...)\
ELPP_WRITE_LOG_EVERY_N(writer, occasion, el::Level::Trace, dispatchAction, __VA_ARGS__)
#else
#  define CTRACE_EVERY_N(writer, occasion, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_TRACE_LOG
#if ELPP_VERBOSE_LOG
#  define CVERBOSE_EVERY_N(writer, occasion, vlevel, dispatchAction, ...)\
CVERBOSE_IF(writer, ELPP->validateEveryNCounter(__FILE__, __LINE__, occasion), vlevel, dispatchAction, __VA_ARGS__)
#else
#  define CVERBOSE_EVERY_N(writer, occasion, vlevel, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_VERBOSE_LOG
// After N logs
#if ELPP_INFO_LOG
#  define CINFO_AFTER_N(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_AFTER_N(writer, n, el::Level::Info, dispatchAction, __VA_ARGS__)
#else
#  define CINFO_AFTER_N(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_INFO_LOG
#if ELPP_WARNING_LOG
#  define CWARNING_AFTER_N(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_AFTER_N(writer, n, el::Level::Warning, dispatchAction, __VA_ARGS__)
#else
#  define CWARNING_AFTER_N(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_WARNING_LOG
#if ELPP_DEBUG_LOG
#  define CDEBUG_AFTER_N(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_AFTER_N(writer, n, el::Level::Debug, dispatchAction, __VA_ARGS__)
#else
#  define CDEBUG_AFTER_N(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_DEBUG_LOG
#if ELPP_ERROR_LOG
#  define CERROR_AFTER_N(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_AFTER_N(writer, n, el::Level::Error, dispatchAction, __VA_ARGS__)
#else
#  define CERROR_AFTER_N(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_ERROR_LOG
#if ELPP_FATAL_LOG
#  define CFATAL_AFTER_N(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_AFTER_N(writer, n, el::Level::Fatal, dispatchAction, __VA_ARGS__)
#else
#  define CFATAL_AFTER_N(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_FATAL_LOG
#if ELPP_TRACE_LOG
#  define CTRACE_AFTER_N(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_AFTER_N(writer, n, el::Level::Trace, dispatchAction, __VA_ARGS__)
#else
#  define CTRACE_AFTER_N(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_TRACE_LOG
#if ELPP_VERBOSE_LOG
#  define CVERBOSE_AFTER_N(writer, n, vlevel, dispatchAction, ...)\
CVERBOSE_IF(writer, ELPP->validateAfterNCounter(__FILE__, __LINE__, n), vlevel, dispatchAction, __VA_ARGS__)
#else
#  define CVERBOSE_AFTER_N(writer, n, vlevel, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_VERBOSE_LOG
// N Times logs
#if ELPP_INFO_LOG
#  define CINFO_N_TIMES(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_N_TIMES(writer, n, el::Level::Info, dispatchAction, __VA_ARGS__)
#else
#  define CINFO_N_TIMES(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_INFO_LOG
#if ELPP_WARNING_LOG
#  define CWARNING_N_TIMES(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_N_TIMES(writer, n, el::Level::Warning, dispatchAction, __VA_ARGS__)
#else
#  define CWARNING_N_TIMES(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_WARNING_LOG
#if ELPP_DEBUG_LOG
#  define CDEBUG_N_TIMES(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_N_TIMES(writer, n, el::Level::Debug, dispatchAction, __VA_ARGS__)
#else
#  define CDEBUG_N_TIMES(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_DEBUG_LOG
#if ELPP_ERROR_LOG
#  define CERROR_N_TIMES(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_N_TIMES(writer, n, el::Level::Error, dispatchAction, __VA_ARGS__)
#else
#  define CERROR_N_TIMES(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_ERROR_LOG
#if ELPP_FATAL_LOG
#  define CFATAL_N_TIMES(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_N_TIMES(writer, n, el::Level::Fatal, dispatchAction, __VA_ARGS__)
#else
#  define CFATAL_N_TIMES(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_FATAL_LOG
#if ELPP_TRACE_LOG
#  define CTRACE_N_TIMES(writer, n, dispatchAction, ...)\
ELPP_WRITE_LOG_N_TIMES(writer, n, el::Level::Trace, dispatchAction, __VA_ARGS__)
#else
#  define CTRACE_N_TIMES(writer, n, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_TRACE_LOG
#if ELPP_VERBOSE_LOG
#  define CVERBOSE_N_TIMES(writer, n, vlevel, dispatchAction, ...)\
CVERBOSE_IF(writer, ELPP->validateNTimesCounter(__FILE__, __LINE__, n), vlevel, dispatchAction, __VA_ARGS__)
#else
#  define CVERBOSE_N_TIMES(writer, n, vlevel, dispatchAction, ...) el::base::NullWriter()
#endif  // ELPP_VERBOSE_LOG
//
// Custom Loggers - Requires (level, dispatchAction, loggerId/s)
//
// undef existing
#undef CLOG
#undef CLOG_VERBOSE
#undef CVLOG
#undef CLOG_IF
#undef CLOG_VERBOSE_IF
#undef CVLOG_IF
#undef CLOG_EVERY_N
#undef CVLOG_EVERY_N
#undef CLOG_AFTER_N
#undef CVLOG_AFTER_N
#undef CLOG_N_TIMES
#undef CVLOG_N_TIMES
// Normal logs
#define CLOG(LEVEL, ...)\
C##LEVEL(el::base::Writer, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CVLOG(vlevel, ...) CVERBOSE(el::base::Writer, vlevel, el::base::DispatchAction::NormalLog, __VA_ARGS__)
// Conditional logs
#define CLOG_IF(condition, LEVEL, ...)\
C##LEVEL##_IF(el::base::Writer, condition, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CVLOG_IF(condition, vlevel, ...)\
CVERBOSE_IF(el::base::Writer, condition, vlevel, el::base::DispatchAction::NormalLog, __VA_ARGS__)
// Hit counts based logs
#define CLOG_EVERY_N(n, LEVEL, ...)\
C##LEVEL##_EVERY_N(el::base::Writer, n, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CVLOG_EVERY_N(n, vlevel, ...)\
CVERBOSE_EVERY_N(el::base::Writer, n, vlevel, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CLOG_AFTER_N(n, LEVEL, ...)\
C##LEVEL##_AFTER_N(el::base::Writer, n, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CVLOG_AFTER_N(n, vlevel, ...)\
CVERBOSE_AFTER_N(el::base::Writer, n, vlevel, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CLOG_N_TIMES(n, LEVEL, ...)\
C##LEVEL##_N_TIMES(el::base::Writer, n, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CVLOG_N_TIMES(n, vlevel, ...)\
CVERBOSE_N_TIMES(el::base::Writer, n, vlevel, el::base::DispatchAction::NormalLog, __VA_ARGS__)
//
// Default Loggers macro using CLOG(), CLOG_VERBOSE() and CVLOG() macros
//
// undef existing
#undef LOG
#undef VLOG
#undef LOG_IF
#undef VLOG_IF
#undef LOG_EVERY_N
#undef VLOG_EVERY_N
#undef LOG_AFTER_N
#undef VLOG_AFTER_N
#undef LOG_N_TIMES
#undef VLOG_N_TIMES
#undef ELPP_CURR_FILE_LOGGER_ID
#if defined(ELPP_DEFAULT_LOGGER)
#  define ELPP_CURR_FILE_LOGGER_ID ELPP_DEFAULT_LOGGER
#else
#  define ELPP_CURR_FILE_LOGGER_ID el::base::consts::kDefaultLoggerId
#endif
#undef ELPP_TRACE
#define ELPP_TRACE CLOG(TRACE, ELPP_CURR_FILE_LOGGER_ID)
// Normal logs
#define LOG(LEVEL) CLOG(LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define VLOG(vlevel) CVLOG(vlevel, ELPP_CURR_FILE_LOGGER_ID)
// Conditional logs
#define LOG_IF(condition, LEVEL) CLOG_IF(condition, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define VLOG_IF(condition, vlevel) CVLOG_IF(condition, vlevel, ELPP_CURR_FILE_LOGGER_ID)
// Hit counts based logs
#define LOG_EVERY_N(n, LEVEL) CLOG_EVERY_N(n, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define VLOG_EVERY_N(n, vlevel) CVLOG_EVERY_N(n, vlevel, ELPP_CURR_FILE_LOGGER_ID)
#define LOG_AFTER_N(n, LEVEL) CLOG_AFTER_N(n, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define VLOG_AFTER_N(n, vlevel) CVLOG_AFTER_N(n, vlevel, ELPP_CURR_FILE_LOGGER_ID)
#define LOG_N_TIMES(n, LEVEL) CLOG_N_TIMES(n, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define VLOG_N_TIMES(n, vlevel) CVLOG_N_TIMES(n, vlevel, ELPP_CURR_FILE_LOGGER_ID)
// Generic PLOG()
#undef CPLOG
#undef CPLOG_IF
#undef PLOG
#undef PLOG_IF
#undef DCPLOG
#undef DCPLOG_IF
#undef DPLOG
#undef DPLOG_IF
#define CPLOG(LEVEL, ...)\
C##LEVEL(el::base::PErrorWriter, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define CPLOG_IF(condition, LEVEL, ...)\
C##LEVEL##_IF(el::base::PErrorWriter, condition, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define DCPLOG(LEVEL, ...)\
if (ELPP_DEBUG_LOG) C##LEVEL(el::base::PErrorWriter, el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define DCPLOG_IF(condition, LEVEL, ...)\
C##LEVEL##_IF(el::base::PErrorWriter, (ELPP_DEBUG_LOG) && (condition), el::base::DispatchAction::NormalLog, __VA_ARGS__)
#define PLOG(LEVEL) CPLOG(LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define PLOG_IF(condition, LEVEL) CPLOG_IF(condition, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DPLOG(LEVEL) DCPLOG(LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DPLOG_IF(condition, LEVEL) DCPLOG_IF(condition, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
// Generic SYSLOG()
#undef CSYSLOG
#undef CSYSLOG_IF
#undef CSYSLOG_EVERY_N
#undef CSYSLOG_AFTER_N
#undef CSYSLOG_N_TIMES
#undef SYSLOG
#undef SYSLOG_IF
#undef SYSLOG_EVERY_N
#undef SYSLOG_AFTER_N
#undef SYSLOG_N_TIMES
#undef DCSYSLOG
#undef DCSYSLOG_IF
#undef DCSYSLOG_EVERY_N
#undef DCSYSLOG_AFTER_N
#undef DCSYSLOG_N_TIMES
#undef DSYSLOG
#undef DSYSLOG_IF
#undef DSYSLOG_EVERY_N
#undef DSYSLOG_AFTER_N
#undef DSYSLOG_N_TIMES
#if defined(ELPP_SYSLOG)
#  define CSYSLOG(LEVEL, ...)\
C##LEVEL(el::base::Writer, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define CSYSLOG_IF(condition, LEVEL, ...)\
C##LEVEL##_IF(el::base::Writer, condition, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define CSYSLOG_EVERY_N(n, LEVEL, ...) C##LEVEL##_EVERY_N(el::base::Writer, n, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define CSYSLOG_AFTER_N(n, LEVEL, ...) C##LEVEL##_AFTER_N(el::base::Writer, n, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define CSYSLOG_N_TIMES(n, LEVEL, ...) C##LEVEL##_N_TIMES(el::base::Writer, n, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define SYSLOG(LEVEL) CSYSLOG(LEVEL, el::base::consts::kSysLogLoggerId)
#  define SYSLOG_IF(condition, LEVEL) CSYSLOG_IF(condition, LEVEL, el::base::consts::kSysLogLoggerId)
#  define SYSLOG_EVERY_N(n, LEVEL) CSYSLOG_EVERY_N(n, LEVEL, el::base::consts::kSysLogLoggerId)
#  define SYSLOG_AFTER_N(n, LEVEL) CSYSLOG_AFTER_N(n, LEVEL, el::base::consts::kSysLogLoggerId)
#  define SYSLOG_N_TIMES(n, LEVEL) CSYSLOG_N_TIMES(n, LEVEL, el::base::consts::kSysLogLoggerId)
#  define DCSYSLOG(LEVEL, ...) if (ELPP_DEBUG_LOG) C##LEVEL(el::base::Writer, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define DCSYSLOG_IF(condition, LEVEL, ...)\
C##LEVEL##_IF(el::base::Writer, (ELPP_DEBUG_LOG) && (condition), el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define DCSYSLOG_EVERY_N(n, LEVEL, ...)\
if (ELPP_DEBUG_LOG) C##LEVEL##_EVERY_N(el::base::Writer, n, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define DCSYSLOG_AFTER_N(n, LEVEL, ...)\
if (ELPP_DEBUG_LOG) C##LEVEL##_AFTER_N(el::base::Writer, n, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define DCSYSLOG_N_TIMES(n, LEVEL, ...)\
if (ELPP_DEBUG_LOG) C##LEVEL##_EVERY_N(el::base::Writer, n, el::base::DispatchAction::SysLog, __VA_ARGS__)
#  define DSYSLOG(LEVEL) DCSYSLOG(LEVEL, el::base::consts::kSysLogLoggerId)
#  define DSYSLOG_IF(condition, LEVEL) DCSYSLOG_IF(condition, LEVEL, el::base::consts::kSysLogLoggerId)
#  define DSYSLOG_EVERY_N(n, LEVEL) DCSYSLOG_EVERY_N(n, LEVEL, el::base::consts::kSysLogLoggerId)
#  define DSYSLOG_AFTER_N(n, LEVEL) DCSYSLOG_AFTER_N(n, LEVEL, el::base::consts::kSysLogLoggerId)
#  define DSYSLOG_N_TIMES(n, LEVEL) DCSYSLOG_N_TIMES(n, LEVEL, el::base::consts::kSysLogLoggerId)
#else
#  define CSYSLOG(LEVEL, ...) el::base::NullWriter()
#  define CSYSLOG_IF(condition, LEVEL, ...) el::base::NullWriter()
#  define CSYSLOG_EVERY_N(n, LEVEL, ...) el::base::NullWriter()
#  define CSYSLOG_AFTER_N(n, LEVEL, ...) el::base::NullWriter()
#  define CSYSLOG_N_TIMES(n, LEVEL, ...) el::base::NullWriter()
#  define SYSLOG(LEVEL) el::base::NullWriter()
#  define SYSLOG_IF(condition, LEVEL) el::base::NullWriter()
#  define SYSLOG_EVERY_N(n, LEVEL) el::base::NullWriter()
#  define SYSLOG_AFTER_N(n, LEVEL) el::base::NullWriter()
#  define SYSLOG_N_TIMES(n, LEVEL) el::base::NullWriter()
#  define DCSYSLOG(LEVEL, ...) el::base::NullWriter()
#  define DCSYSLOG_IF(condition, LEVEL, ...) el::base::NullWriter()
#  define DCSYSLOG_EVERY_N(n, LEVEL, ...) el::base::NullWriter()
#  define DCSYSLOG_AFTER_N(n, LEVEL, ...) el::base::NullWriter()
#  define DCSYSLOG_N_TIMES(n, LEVEL, ...) el::base::NullWriter()
#  define DSYSLOG(LEVEL) el::base::NullWriter()
#  define DSYSLOG_IF(condition, LEVEL) el::base::NullWriter()
#  define DSYSLOG_EVERY_N(n, LEVEL) el::base::NullWriter()
#  define DSYSLOG_AFTER_N(n, LEVEL) el::base::NullWriter()
#  define DSYSLOG_N_TIMES(n, LEVEL) el::base::NullWriter()
#endif  // defined(ELPP_SYSLOG)
//
// Custom Debug Only Loggers - Requires (level, loggerId/s)
//
// undef existing
#undef DCLOG
#undef DCVLOG
#undef DCLOG_IF
#undef DCVLOG_IF
#undef DCLOG_EVERY_N
#undef DCVLOG_EVERY_N
#undef DCLOG_AFTER_N
#undef DCVLOG_AFTER_N
#undef DCLOG_N_TIMES
#undef DCVLOG_N_TIMES
// Normal logs
#define DCLOG(LEVEL, ...) if (ELPP_DEBUG_LOG) CLOG(LEVEL, __VA_ARGS__)
#define DCLOG_VERBOSE(vlevel, ...) if (ELPP_DEBUG_LOG) CLOG_VERBOSE(vlevel, __VA_ARGS__)
#define DCVLOG(vlevel, ...) if (ELPP_DEBUG_LOG) CVLOG(vlevel, __VA_ARGS__)
// Conditional logs
#define DCLOG_IF(condition, LEVEL, ...) if (ELPP_DEBUG_LOG) CLOG_IF(condition, LEVEL, __VA_ARGS__)
#define DCVLOG_IF(condition, vlevel, ...) if (ELPP_DEBUG_LOG) CVLOG_IF(condition, vlevel, __VA_ARGS__)
// Hit counts based logs
#define DCLOG_EVERY_N(n, LEVEL, ...) if (ELPP_DEBUG_LOG) CLOG_EVERY_N(n, LEVEL, __VA_ARGS__)
#define DCVLOG_EVERY_N(n, vlevel, ...) if (ELPP_DEBUG_LOG) CVLOG_EVERY_N(n, vlevel, __VA_ARGS__)
#define DCLOG_AFTER_N(n, LEVEL, ...) if (ELPP_DEBUG_LOG) CLOG_AFTER_N(n, LEVEL, __VA_ARGS__)
#define DCVLOG_AFTER_N(n, vlevel, ...) if (ELPP_DEBUG_LOG) CVLOG_AFTER_N(n, vlevel, __VA_ARGS__)
#define DCLOG_N_TIMES(n, LEVEL, ...) if (ELPP_DEBUG_LOG) CLOG_N_TIMES(n, LEVEL, __VA_ARGS__)
#define DCVLOG_N_TIMES(n, vlevel, ...) if (ELPP_DEBUG_LOG) CVLOG_N_TIMES(n, vlevel, __VA_ARGS__)
//
// Default Debug Only Loggers macro using CLOG(), CLOG_VERBOSE() and CVLOG() macros
//
#if !defined(ELPP_NO_DEBUG_MACROS)
// undef existing
#undef DLOG
#undef DVLOG
#undef DLOG_IF
#undef DVLOG_IF
#undef DLOG_EVERY_N
#undef DVLOG_EVERY_N
#undef DLOG_AFTER_N
#undef DVLOG_AFTER_N
#undef DLOG_N_TIMES
#undef DVLOG_N_TIMES
// Normal logs
#define DLOG(LEVEL) DCLOG(LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DVLOG(vlevel) DCVLOG(vlevel, ELPP_CURR_FILE_LOGGER_ID)
// Conditional logs
#define DLOG_IF(condition, LEVEL) DCLOG_IF(condition, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DVLOG_IF(condition, vlevel) DCVLOG_IF(condition, vlevel, ELPP_CURR_FILE_LOGGER_ID)
// Hit counts based logs
#define DLOG_EVERY_N(n, LEVEL) DCLOG_EVERY_N(n, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DVLOG_EVERY_N(n, vlevel) DCVLOG_EVERY_N(n, vlevel, ELPP_CURR_FILE_LOGGER_ID)
#define DLOG_AFTER_N(n, LEVEL) DCLOG_AFTER_N(n, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DVLOG_AFTER_N(n, vlevel) DCVLOG_AFTER_N(n, vlevel, ELPP_CURR_FILE_LOGGER_ID)
#define DLOG_N_TIMES(n, LEVEL) DCLOG_N_TIMES(n, LEVEL, ELPP_CURR_FILE_LOGGER_ID)
#define DVLOG_N_TIMES(n, vlevel) DCVLOG_N_TIMES(n, vlevel, ELPP_CURR_FILE_LOGGER_ID)
#endif // defined(ELPP_NO_DEBUG_MACROS)
#if !defined(ELPP_NO_CHECK_MACROS)
// Check macros
#undef CCHECK
#undef CPCHECK
#undef CCHECK_EQ
#undef CCHECK_NE
#undef CCHECK_LT
#undef CCHECK_GT
#undef CCHECK_LE
#undef CCHECK_GE
#undef CCHECK_BOUNDS
#undef CCHECK_NOTNULL
#undef CCHECK_STRCASEEQ
#undef CCHECK_STRCASENE
#undef CHECK
#undef PCHECK
#undef CHECK_EQ
#undef CHECK_NE
#undef CHECK_LT
#undef CHECK_GT
#undef CHECK_LE
#undef CHECK_GE
#undef CHECK_BOUNDS
#undef CHECK_NOTNULL
#undef CHECK_STRCASEEQ
#undef CHECK_STRCASENE
#define CCHECK(condition, ...) CLOG_IF(!(condition), FATAL, __VA_ARGS__) << "Check failed: [" << #condition << "] "
#define CPCHECK(condition, ...) CPLOG_IF(!(condition), FATAL, __VA_ARGS__) << "Check failed: [" << #condition << "] "
#define CHECK(condition) CCHECK(condition, ELPP_CURR_FILE_LOGGER_ID)
#define PCHECK(condition) CPCHECK(condition, ELPP_CURR_FILE_LOGGER_ID)
#define CCHECK_EQ(a, b, ...) CCHECK(a == b, __VA_ARGS__)
#define CCHECK_NE(a, b, ...) CCHECK(a != b, __VA_ARGS__)
#define CCHECK_LT(a, b, ...) CCHECK(a < b, __VA_ARGS__)
#define CCHECK_GT(a, b, ...) CCHECK(a > b, __VA_ARGS__)
#define CCHECK_LE(a, b, ...) CCHECK(a <= b, __VA_ARGS__)
#define CCHECK_GE(a, b, ...) CCHECK(a >= b, __VA_ARGS__)
#define CCHECK_BOUNDS(val, min, max, ...) CCHECK(val >= min && val <= max, __VA_ARGS__)
#define CHECK_EQ(a, b) CCHECK_EQ(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_NE(a, b) CCHECK_NE(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_LT(a, b) CCHECK_LT(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_GT(a, b) CCHECK_GT(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_LE(a, b) CCHECK_LE(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_GE(a, b) CCHECK_GE(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_BOUNDS(val, min, max) CCHECK_BOUNDS(val, min, max, ELPP_CURR_FILE_LOGGER_ID)
#define CCHECK_NOTNULL(ptr, ...) CCHECK((ptr) != nullptr, __VA_ARGS__)
#define CCHECK_STREQ(str1, str2, ...) CLOG_IF(!el::base::utils::Str::cStringEq(str1, str2), FATAL, __VA_ARGS__) \
<< "Check failed: [" << #str1 << " == " << #str2 << "] "
#define CCHECK_STRNE(str1, str2, ...) CLOG_IF(el::base::utils::Str::cStringEq(str1, str2), FATAL, __VA_ARGS__) \
<< "Check failed: [" << #str1 << " != " << #str2 << "] "
#define CCHECK_STRCASEEQ(str1, str2, ...) CLOG_IF(!el::base::utils::Str::cStringCaseEq(str1, str2), FATAL, __VA_ARGS__) \
<< "Check failed: [" << #str1 << " == " << #str2 << "] "
#define CCHECK_STRCASENE(str1, str2, ...) CLOG_IF(el::base::utils::Str::cStringCaseEq(str1, str2), FATAL, __VA_ARGS__) \
<< "Check failed: [" << #str1 << " != " << #str2 << "] "
#define CHECK_NOTNULL(ptr) CCHECK_NOTNULL((ptr), ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_STREQ(str1, str2) CCHECK_STREQ(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_STRNE(str1, str2) CCHECK_STRNE(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_STRCASEEQ(str1, str2) CCHECK_STRCASEEQ(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define CHECK_STRCASENE(str1, str2) CCHECK_STRCASENE(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#undef DCCHECK
#undef DCCHECK_EQ
#undef DCCHECK_NE
#undef DCCHECK_LT
#undef DCCHECK_GT
#undef DCCHECK_LE
#undef DCCHECK_GE
#undef DCCHECK_BOUNDS
#undef DCCHECK_NOTNULL
#undef DCCHECK_STRCASEEQ
#undef DCCHECK_STRCASENE
#undef DCPCHECK
#undef DCHECK
#undef DCHECK_EQ
#undef DCHECK_NE
#undef DCHECK_LT
#undef DCHECK_GT
#undef DCHECK_LE
#undef DCHECK_GE
#undef DCHECK_BOUNDS_
#undef DCHECK_NOTNULL
#undef DCHECK_STRCASEEQ
#undef DCHECK_STRCASENE
#undef DPCHECK
#define DCCHECK(condition, ...) if (ELPP_DEBUG_LOG) CCHECK(condition, __VA_ARGS__)
#define DCCHECK_EQ(a, b, ...) if (ELPP_DEBUG_LOG) CCHECK_EQ(a, b, __VA_ARGS__)
#define DCCHECK_NE(a, b, ...) if (ELPP_DEBUG_LOG) CCHECK_NE(a, b, __VA_ARGS__)
#define DCCHECK_LT(a, b, ...) if (ELPP_DEBUG_LOG) CCHECK_LT(a, b, __VA_ARGS__)
#define DCCHECK_GT(a, b, ...) if (ELPP_DEBUG_LOG) CCHECK_GT(a, b, __VA_ARGS__)
#define DCCHECK_LE(a, b, ...) if (ELPP_DEBUG_LOG) CCHECK_LE(a, b, __VA_ARGS__)
#define DCCHECK_GE(a, b, ...) if (ELPP_DEBUG_LOG) CCHECK_GE(a, b, __VA_ARGS__)
#define DCCHECK_BOUNDS(val, min, max, ...) if (ELPP_DEBUG_LOG) CCHECK_BOUNDS(val, min, max, __VA_ARGS__)
#define DCCHECK_NOTNULL(ptr, ...) if (ELPP_DEBUG_LOG) CCHECK_NOTNULL((ptr), __VA_ARGS__)
#define DCCHECK_STREQ(str1, str2, ...) if (ELPP_DEBUG_LOG) CCHECK_STREQ(str1, str2, __VA_ARGS__)
#define DCCHECK_STRNE(str1, str2, ...) if (ELPP_DEBUG_LOG) CCHECK_STRNE(str1, str2, __VA_ARGS__)
#define DCCHECK_STRCASEEQ(str1, str2, ...) if (ELPP_DEBUG_LOG) CCHECK_STRCASEEQ(str1, str2, __VA_ARGS__)
#define DCCHECK_STRCASENE(str1, str2, ...) if (ELPP_DEBUG_LOG) CCHECK_STRCASENE(str1, str2, __VA_ARGS__)
#define DCPCHECK(condition, ...) if (ELPP_DEBUG_LOG) CPCHECK(condition, __VA_ARGS__)
#define DCHECK(condition) DCCHECK(condition, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_EQ(a, b) DCCHECK_EQ(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_NE(a, b) DCCHECK_NE(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_LT(a, b) DCCHECK_LT(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_GT(a, b) DCCHECK_GT(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_LE(a, b) DCCHECK_LE(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_GE(a, b) DCCHECK_GE(a, b, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_BOUNDS(val, min, max) DCCHECK_BOUNDS(val, min, max, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_NOTNULL(ptr) DCCHECK_NOTNULL((ptr), ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_STREQ(str1, str2) DCCHECK_STREQ(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_STRNE(str1, str2) DCCHECK_STRNE(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_STRCASEEQ(str1, str2) DCCHECK_STRCASEEQ(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define DCHECK_STRCASENE(str1, str2) DCCHECK_STRCASENE(str1, str2, ELPP_CURR_FILE_LOGGER_ID)
#define DPCHECK(condition) DCPCHECK(condition, ELPP_CURR_FILE_LOGGER_ID)
#endif // defined(ELPP_NO_CHECK_MACROS)
#if defined(ELPP_DISABLE_DEFAULT_CRASH_HANDLING)
#  define ELPP_USE_DEF_CRASH_HANDLER false
#else
#  define ELPP_USE_DEF_CRASH_HANDLER true
#endif  // defined(ELPP_DISABLE_DEFAULT_CRASH_HANDLING)
#define ELPP_CRASH_HANDLER_INIT
#define ELPP_INIT_EASYLOGGINGPP(val) \
namespace el { \
namespace base { \
el::base::type::StoragePointer elStorage(val); \
} \
el::base::debug::CrashHandler elCrashHandler(ELPP_USE_DEF_CRASH_HANDLER); \
}

#if ELPP_ASYNC_LOGGING
#  define INITIALIZE_EASYLOGGINGPP ELPP_INIT_EASYLOGGINGPP(new el::base::Storage(el::LogBuilderPtr(new el::base::DefaultLogBuilder()),\
new el::base::AsyncDispatchWorker()))
#else
#  define INITIALIZE_EASYLOGGINGPP ELPP_INIT_EASYLOGGINGPP(new el::base::Storage(el::LogBuilderPtr(new el::base::DefaultLogBuilder())))
#endif  // ELPP_ASYNC_LOGGING
#define INITIALIZE_NULL_EASYLOGGINGPP \
namespace el {\
namespace base {\
el::base::type::StoragePointer elStorage;\
}\
el::base::debug::CrashHandler elCrashHandler(ELPP_USE_DEF_CRASH_HANDLER);\
}
#define SHARE_EASYLOGGINGPP(initializedStorage)\
namespace el {\
namespace base {\
el::base::type::StoragePointer elStorage(initializedStorage);\
}\
el::base::debug::CrashHandler elCrashHandler(ELPP_USE_DEF_CRASH_HANDLER);\
}

#if defined(ELPP_UNICODE)
#  define START_EASYLOGGINGPP(argc, argv) el::Helpers::setArgs(argc, argv); std::locale::global(std::locale(""))
#else
#  define START_EASYLOGGINGPP(argc, argv) el::Helpers::setArgs(argc, argv)
#endif  // defined(ELPP_UNICODE)
#endif // EASYLOGGINGPP_H
