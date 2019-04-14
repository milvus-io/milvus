//
//  Bismillah ar-Rahmaan ar-Raheem
//
//  Easylogging++ v9.96.7
//  Cross-platform logging library for C++ applications
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

#include "easylogging++.h"

#if defined(AUTO_INITIALIZE_EASYLOGGINGPP)
INITIALIZE_EASYLOGGINGPP
#endif

namespace el {

// el::base
namespace base {
// el::base::consts
namespace consts {

// Level log values - These are values that are replaced in place of %level format specifier
// Extra spaces after format specifiers are only for readability purposes in log files
static const base::type::char_t* kInfoLevelLogValue     =   ELPP_LITERAL("INFO");
static const base::type::char_t* kDebugLevelLogValue    =   ELPP_LITERAL("DEBUG");
static const base::type::char_t* kWarningLevelLogValue  =   ELPP_LITERAL("WARNING");
static const base::type::char_t* kErrorLevelLogValue    =   ELPP_LITERAL("ERROR");
static const base::type::char_t* kFatalLevelLogValue    =   ELPP_LITERAL("FATAL");
static const base::type::char_t* kVerboseLevelLogValue  =
  ELPP_LITERAL("VERBOSE"); // will become VERBOSE-x where x = verbose level
static const base::type::char_t* kTraceLevelLogValue    =   ELPP_LITERAL("TRACE");
static const base::type::char_t* kInfoLevelShortLogValue     =   ELPP_LITERAL("I");
static const base::type::char_t* kDebugLevelShortLogValue    =   ELPP_LITERAL("D");
static const base::type::char_t* kWarningLevelShortLogValue  =   ELPP_LITERAL("W");
static const base::type::char_t* kErrorLevelShortLogValue    =   ELPP_LITERAL("E");
static const base::type::char_t* kFatalLevelShortLogValue    =   ELPP_LITERAL("F");
static const base::type::char_t* kVerboseLevelShortLogValue  =   ELPP_LITERAL("V");
static const base::type::char_t* kTraceLevelShortLogValue    =   ELPP_LITERAL("T");
// Format specifiers - These are used to define log format
static const base::type::char_t* kAppNameFormatSpecifier          =      ELPP_LITERAL("%app");
static const base::type::char_t* kLoggerIdFormatSpecifier         =      ELPP_LITERAL("%logger");
static const base::type::char_t* kThreadIdFormatSpecifier         =      ELPP_LITERAL("%thread");
static const base::type::char_t* kSeverityLevelFormatSpecifier    =      ELPP_LITERAL("%level");
static const base::type::char_t* kSeverityLevelShortFormatSpecifier    =      ELPP_LITERAL("%levshort");
static const base::type::char_t* kDateTimeFormatSpecifier         =      ELPP_LITERAL("%datetime");
static const base::type::char_t* kLogFileFormatSpecifier          =      ELPP_LITERAL("%file");
static const base::type::char_t* kLogFileBaseFormatSpecifier      =      ELPP_LITERAL("%fbase");
static const base::type::char_t* kLogLineFormatSpecifier          =      ELPP_LITERAL("%line");
static const base::type::char_t* kLogLocationFormatSpecifier      =      ELPP_LITERAL("%loc");
static const base::type::char_t* kLogFunctionFormatSpecifier      =      ELPP_LITERAL("%func");
static const base::type::char_t* kCurrentUserFormatSpecifier      =      ELPP_LITERAL("%user");
static const base::type::char_t* kCurrentHostFormatSpecifier      =      ELPP_LITERAL("%host");
static const base::type::char_t* kMessageFormatSpecifier          =      ELPP_LITERAL("%msg");
static const base::type::char_t* kVerboseLevelFormatSpecifier     =      ELPP_LITERAL("%vlevel");
static const char* kDateTimeFormatSpecifierForFilename            =      "%datetime";
// Date/time
static const char* kDays[7]                         =      { "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };
static const char* kDaysAbbrev[7]                   =      { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
static const char* kMonths[12]                      =      { "January", "February", "March", "Apri", "May", "June", "July", "August",
                                                             "September", "October", "November", "December"
                                                           };
static const char* kMonthsAbbrev[12]                =      { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
static const char* kDefaultDateTimeFormat           =      "%Y-%M-%d %H:%m:%s,%g";
static const char* kDefaultDateTimeFormatInFilename =      "%Y-%M-%d_%H-%m";
static const int kYearBase                          =      1900;
static const char* kAm                              =      "AM";
static const char* kPm                              =      "PM";
// Miscellaneous constants

static const char* kNullPointer                            =      "nullptr";
#if ELPP_VARIADIC_TEMPLATES_SUPPORTED
#endif  // ELPP_VARIADIC_TEMPLATES_SUPPORTED
static const base::type::VerboseLevel kMaxVerboseLevel     =      9;
static const char* kUnknownUser                            =      "user";
static const char* kUnknownHost                            =      "unknown-host";


//---------------- DEFAULT LOG FILE -----------------------

#if defined(ELPP_NO_DEFAULT_LOG_FILE)
#  if ELPP_OS_UNIX
static const char* kDefaultLogFile                         =      "/dev/null";
#  elif ELPP_OS_WINDOWS
static const char* kDefaultLogFile                         =      "nul";
#  endif  // ELPP_OS_UNIX
#elif defined(ELPP_DEFAULT_LOG_FILE)
static const char* kDefaultLogFile                         =      ELPP_DEFAULT_LOG_FILE;
#else
static const char* kDefaultLogFile                         =      "myeasylog.log";
#endif // defined(ELPP_NO_DEFAULT_LOG_FILE)


#if !defined(ELPP_DISABLE_LOG_FILE_FROM_ARG)
static const char* kDefaultLogFileParam                    =      "--default-log-file";
#endif  // !defined(ELPP_DISABLE_LOG_FILE_FROM_ARG)
#if defined(ELPP_LOGGING_FLAGS_FROM_ARG)
static const char* kLoggingFlagsParam                      =      "--logging-flags";
#endif  // defined(ELPP_LOGGING_FLAGS_FROM_ARG)
static const char* kValidLoggerIdSymbols                   =
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._";
static const char* kConfigurationComment                   =      "##";
static const char* kConfigurationLevel                     =      "*";
static const char* kConfigurationLoggerId                  =      "--";
}
// el::base::utils
namespace utils {

/// @brief Aborts application due with user-defined status
static void abort(int status, const std::string& reason) {
  // Both status and reason params are there for debugging with tools like gdb etc
  ELPP_UNUSED(status);
  ELPP_UNUSED(reason);
#if defined(ELPP_COMPILER_MSVC) && defined(_M_IX86) && defined(_DEBUG)
  // Ignore msvc critical error dialog - break instead (on debug mode)
  _asm int 3
#else
  ::abort();
#endif  // defined(ELPP_COMPILER_MSVC) && defined(_M_IX86) && defined(_DEBUG)
}

} // namespace utils
} // namespace base

// el

// LevelHelper

const char* LevelHelper::convertToString(Level level) {
  // Do not use switch over strongly typed enums because Intel C++ compilers dont support them yet.
  if (level == Level::Global) return "GLOBAL";
  if (level == Level::Debug) return "DEBUG";
  if (level == Level::Info) return "INFO";
  if (level == Level::Warning) return "WARNING";
  if (level == Level::Error) return "ERROR";
  if (level == Level::Fatal) return "FATAL";
  if (level == Level::Verbose) return "VERBOSE";
  if (level == Level::Trace) return "TRACE";
  return "UNKNOWN";
}

struct StringToLevelItem {
  const char* levelString;
  Level level;
};

static struct StringToLevelItem stringToLevelMap[] = {
  { "global", Level::Global },
  { "debug", Level::Debug },
  { "info", Level::Info },
  { "warning", Level::Warning },
  { "error", Level::Error },
  { "fatal", Level::Fatal },
  { "verbose", Level::Verbose },
  { "trace", Level::Trace }
};

Level LevelHelper::convertFromString(const char* levelStr) {
  for (auto& item : stringToLevelMap) {
    if (base::utils::Str::cStringCaseEq(levelStr, item.levelString)) {
      return item.level;
    }
  }
  return Level::Unknown;
}

void LevelHelper::forEachLevel(base::type::EnumType* startIndex, const std::function<bool(void)>& fn) {
  base::type::EnumType lIndexMax = LevelHelper::kMaxValid;
  do {
    if (fn()) {
      break;
    }
    *startIndex = static_cast<base::type::EnumType>(*startIndex << 1);
  } while (*startIndex <= lIndexMax);
}

// ConfigurationTypeHelper

const char* ConfigurationTypeHelper::convertToString(ConfigurationType configurationType) {
  // Do not use switch over strongly typed enums because Intel C++ compilers dont support them yet.
  if (configurationType == ConfigurationType::Enabled) return "ENABLED";
  if (configurationType == ConfigurationType::Filename) return "FILENAME";
  if (configurationType == ConfigurationType::Format) return "FORMAT";
  if (configurationType == ConfigurationType::ToFile) return "TO_FILE";
  if (configurationType == ConfigurationType::ToStandardOutput) return "TO_STANDARD_OUTPUT";
  if (configurationType == ConfigurationType::SubsecondPrecision) return "SUBSECOND_PRECISION";
  if (configurationType == ConfigurationType::PerformanceTracking) return "PERFORMANCE_TRACKING";
  if (configurationType == ConfigurationType::MaxLogFileSize) return "MAX_LOG_FILE_SIZE";
  if (configurationType == ConfigurationType::LogFlushThreshold) return "LOG_FLUSH_THRESHOLD";
  return "UNKNOWN";
}

struct ConfigurationStringToTypeItem {
  const char* configString;
  ConfigurationType configType;
};

static struct ConfigurationStringToTypeItem configStringToTypeMap[] = {
  { "enabled", ConfigurationType::Enabled },
  { "to_file", ConfigurationType::ToFile },
  { "to_standard_output", ConfigurationType::ToStandardOutput },
  { "format", ConfigurationType::Format },
  { "filename", ConfigurationType::Filename },
  { "subsecond_precision", ConfigurationType::SubsecondPrecision },
  { "milliseconds_width", ConfigurationType::MillisecondsWidth },
  { "performance_tracking", ConfigurationType::PerformanceTracking },
  { "max_log_file_size", ConfigurationType::MaxLogFileSize },
  { "log_flush_threshold", ConfigurationType::LogFlushThreshold },
};

ConfigurationType ConfigurationTypeHelper::convertFromString(const char* configStr) {
  for (auto& item : configStringToTypeMap) {
    if (base::utils::Str::cStringCaseEq(configStr, item.configString)) {
      return item.configType;
    }
  }
  return ConfigurationType::Unknown;
}

void ConfigurationTypeHelper::forEachConfigType(base::type::EnumType* startIndex, const std::function<bool(void)>& fn) {
  base::type::EnumType cIndexMax = ConfigurationTypeHelper::kMaxValid;
  do {
    if (fn()) {
      break;
    }
    *startIndex = static_cast<base::type::EnumType>(*startIndex << 1);
  } while (*startIndex <= cIndexMax);
}

// Configuration

Configuration::Configuration(const Configuration& c) :
  m_level(c.m_level),
  m_configurationType(c.m_configurationType),
  m_value(c.m_value) {
}

Configuration& Configuration::operator=(const Configuration& c) {
  if (&c != this) {
    m_level = c.m_level;
    m_configurationType = c.m_configurationType;
    m_value = c.m_value;
  }
  return *this;
}

/// @brief Full constructor used to sets value of configuration
Configuration::Configuration(Level level, ConfigurationType configurationType, const std::string& value) :
  m_level(level),
  m_configurationType(configurationType),
  m_value(value) {
}

void Configuration::log(el::base::type::ostream_t& os) const {
  os << LevelHelper::convertToString(m_level)
     << ELPP_LITERAL(" ") << ConfigurationTypeHelper::convertToString(m_configurationType)
     << ELPP_LITERAL(" = ") << m_value.c_str();
}

/// @brief Used to find configuration from configuration (pointers) repository. Avoid using it.
Configuration::Predicate::Predicate(Level level, ConfigurationType configurationType) :
  m_level(level),
  m_configurationType(configurationType) {
}

bool Configuration::Predicate::operator()(const Configuration* conf) const {
  return ((conf != nullptr) && (conf->level() == m_level) && (conf->configurationType() == m_configurationType));
}

// Configurations

Configurations::Configurations(void) :
  m_configurationFile(std::string()),
  m_isFromFile(false) {
}

Configurations::Configurations(const std::string& configurationFile, bool useDefaultsForRemaining,
                               Configurations* base) :
  m_configurationFile(configurationFile),
  m_isFromFile(false) {
  parseFromFile(configurationFile, base);
  if (useDefaultsForRemaining) {
    setRemainingToDefault();
  }
}

bool Configurations::parseFromFile(const std::string& configurationFile, Configurations* base) {
  // We initial assertion with true because if we have assertion diabled, we want to pass this
  // check and if assertion is enabled we will have values re-assigned any way.
  bool assertionPassed = true;
  ELPP_ASSERT((assertionPassed = base::utils::File::pathExists(configurationFile.c_str(), true)) == true,
              "Configuration file [" << configurationFile << "] does not exist!");
  if (!assertionPassed) {
    return false;
  }
  bool success = Parser::parseFromFile(configurationFile, this, base);
  m_isFromFile = success;
  return success;
}

bool Configurations::parseFromText(const std::string& configurationsString, Configurations* base) {
  bool success = Parser::parseFromText(configurationsString, this, base);
  if (success) {
    m_isFromFile = false;
  }
  return success;
}

void Configurations::setFromBase(Configurations* base) {
  if (base == nullptr || base == this) {
    return;
  }
  base::threading::ScopedLock scopedLock(base->lock());
  for (Configuration*& conf : base->list()) {
    set(conf);
  }
}

bool Configurations::hasConfiguration(ConfigurationType configurationType) {
  base::type::EnumType lIndex = LevelHelper::kMinValid;
  bool result = false;
  LevelHelper::forEachLevel(&lIndex, [&](void) -> bool {
    if (hasConfiguration(LevelHelper::castFromInt(lIndex), configurationType)) {
      result = true;
    }
    return result;
  });
  return result;
}

bool Configurations::hasConfiguration(Level level, ConfigurationType configurationType) {
  base::threading::ScopedLock scopedLock(lock());
#if ELPP_COMPILER_INTEL
  // We cant specify template types here, Intel C++ throws compilation error
  // "error: type name is not allowed"
  return RegistryWithPred::get(level, configurationType) != nullptr;
#else
  return RegistryWithPred<Configuration, Configuration::Predicate>::get(level, configurationType) != nullptr;
#endif  // ELPP_COMPILER_INTEL
}

void Configurations::set(Level level, ConfigurationType configurationType, const std::string& value) {
  base::threading::ScopedLock scopedLock(lock());
  unsafeSet(level, configurationType, value);  // This is not unsafe anymore as we have locked mutex
  if (level == Level::Global) {
    unsafeSetGlobally(configurationType, value, false);  // Again this is not unsafe either
  }
}

void Configurations::set(Configuration* conf) {
  if (conf == nullptr) {
    return;
  }
  set(conf->level(), conf->configurationType(), conf->value());
}

void Configurations::setToDefault(void) {
  setGlobally(ConfigurationType::Enabled, std::string("true"), true);
  setGlobally(ConfigurationType::Filename, std::string(base::consts::kDefaultLogFile), true);
#if defined(ELPP_NO_LOG_TO_FILE)
  setGlobally(ConfigurationType::ToFile, std::string("false"), true);
#else
  setGlobally(ConfigurationType::ToFile, std::string("true"), true);
#endif // defined(ELPP_NO_LOG_TO_FILE)
  setGlobally(ConfigurationType::ToStandardOutput, std::string("true"), true);
  setGlobally(ConfigurationType::SubsecondPrecision, std::string("3"), true);
  setGlobally(ConfigurationType::PerformanceTracking, std::string("true"), true);
  setGlobally(ConfigurationType::MaxLogFileSize, std::string("0"), true);
  setGlobally(ConfigurationType::LogFlushThreshold, std::string("0"), true);

  setGlobally(ConfigurationType::Format, std::string("%datetime %level [%logger] %msg"), true);
  set(Level::Debug, ConfigurationType::Format,
      std::string("%datetime %level [%logger] [%user@%host] [%func] [%loc] %msg"));
  // INFO and WARNING are set to default by Level::Global
  set(Level::Error, ConfigurationType::Format, std::string("%datetime %level [%logger] %msg"));
  set(Level::Fatal, ConfigurationType::Format, std::string("%datetime %level [%logger] %msg"));
  set(Level::Verbose, ConfigurationType::Format, std::string("%datetime %level-%vlevel [%logger] %msg"));
  set(Level::Trace, ConfigurationType::Format, std::string("%datetime %level [%logger] [%func] [%loc] %msg"));
}

void Configurations::setRemainingToDefault(void) {
  base::threading::ScopedLock scopedLock(lock());
#if defined(ELPP_NO_LOG_TO_FILE)
  unsafeSetIfNotExist(Level::Global, ConfigurationType::Enabled, std::string("false"));
#else
  unsafeSetIfNotExist(Level::Global, ConfigurationType::Enabled, std::string("true"));
#endif // defined(ELPP_NO_LOG_TO_FILE)
  unsafeSetIfNotExist(Level::Global, ConfigurationType::Filename, std::string(base::consts::kDefaultLogFile));
  unsafeSetIfNotExist(Level::Global, ConfigurationType::ToStandardOutput, std::string("true"));
  unsafeSetIfNotExist(Level::Global, ConfigurationType::SubsecondPrecision, std::string("3"));
  unsafeSetIfNotExist(Level::Global, ConfigurationType::PerformanceTracking, std::string("true"));
  unsafeSetIfNotExist(Level::Global, ConfigurationType::MaxLogFileSize, std::string("0"));
  unsafeSetIfNotExist(Level::Global, ConfigurationType::Format, std::string("%datetime %level [%logger] %msg"));
  unsafeSetIfNotExist(Level::Debug, ConfigurationType::Format,
                      std::string("%datetime %level [%logger] [%user@%host] [%func] [%loc] %msg"));
  // INFO and WARNING are set to default by Level::Global
  unsafeSetIfNotExist(Level::Error, ConfigurationType::Format, std::string("%datetime %level [%logger] %msg"));
  unsafeSetIfNotExist(Level::Fatal, ConfigurationType::Format, std::string("%datetime %level [%logger] %msg"));
  unsafeSetIfNotExist(Level::Verbose, ConfigurationType::Format, std::string("%datetime %level-%vlevel [%logger] %msg"));
  unsafeSetIfNotExist(Level::Trace, ConfigurationType::Format,
                      std::string("%datetime %level [%logger] [%func] [%loc] %msg"));
}

bool Configurations::Parser::parseFromFile(const std::string& configurationFile, Configurations* sender,
    Configurations* base) {
  sender->setFromBase(base);
  std::ifstream fileStream_(configurationFile.c_str(), std::ifstream::in);
  ELPP_ASSERT(fileStream_.is_open(), "Unable to open configuration file [" << configurationFile << "] for parsing.");
  bool parsedSuccessfully = false;
  std::string line = std::string();
  Level currLevel = Level::Unknown;
  std::string currConfigStr = std::string();
  std::string currLevelStr = std::string();
  while (fileStream_.good()) {
    std::getline(fileStream_, line);
    parsedSuccessfully = parseLine(&line, &currConfigStr, &currLevelStr, &currLevel, sender);
    ELPP_ASSERT(parsedSuccessfully, "Unable to parse configuration line: " << line);
  }
  return parsedSuccessfully;
}

bool Configurations::Parser::parseFromText(const std::string& configurationsString, Configurations* sender,
    Configurations* base) {
  sender->setFromBase(base);
  bool parsedSuccessfully = false;
  std::stringstream ss(configurationsString);
  std::string line = std::string();
  Level currLevel = Level::Unknown;
  std::string currConfigStr = std::string();
  std::string currLevelStr = std::string();
  while (std::getline(ss, line)) {
    parsedSuccessfully = parseLine(&line, &currConfigStr, &currLevelStr, &currLevel, sender);
    ELPP_ASSERT(parsedSuccessfully, "Unable to parse configuration line: " << line);
  }
  return parsedSuccessfully;
}

void Configurations::Parser::ignoreComments(std::string* line) {
  std::size_t foundAt = 0;
  std::size_t quotesStart = line->find("\"");
  std::size_t quotesEnd = std::string::npos;
  if (quotesStart != std::string::npos) {
    quotesEnd = line->find("\"", quotesStart + 1);
    while (quotesEnd != std::string::npos && line->at(quotesEnd - 1) == '\\') {
      // Do not erase slash yet - we will erase it in parseLine(..) while loop
      quotesEnd = line->find("\"", quotesEnd + 2);
    }
  }
  if ((foundAt = line->find(base::consts::kConfigurationComment)) != std::string::npos) {
    if (foundAt < quotesEnd) {
      foundAt = line->find(base::consts::kConfigurationComment, quotesEnd + 1);
    }
    *line = line->substr(0, foundAt);
  }
}

bool Configurations::Parser::isLevel(const std::string& line) {
  return base::utils::Str::startsWith(line, std::string(base::consts::kConfigurationLevel));
}

bool Configurations::Parser::isComment(const std::string& line) {
  return base::utils::Str::startsWith(line, std::string(base::consts::kConfigurationComment));
}

bool Configurations::Parser::isConfig(const std::string& line) {
  std::size_t assignment = line.find('=');
  return line != "" &&
         ((line[0] >= 'A' && line[0] <= 'Z') || (line[0] >= 'a' && line[0] <= 'z')) &&
         (assignment != std::string::npos) &&
         (line.size() > assignment);
}

bool Configurations::Parser::parseLine(std::string* line, std::string* currConfigStr, std::string* currLevelStr,
                                       Level* currLevel,
                                       Configurations* conf) {
  ConfigurationType currConfig = ConfigurationType::Unknown;
  std::string currValue = std::string();
  *line = base::utils::Str::trim(*line);
  if (isComment(*line)) return true;
  ignoreComments(line);
  *line = base::utils::Str::trim(*line);
  if (line->empty()) {
    // Comment ignored
    return true;
  }
  if (isLevel(*line)) {
    if (line->size() <= 2) {
      return true;
    }
    *currLevelStr = line->substr(1, line->size() - 2);
    *currLevelStr = base::utils::Str::toUpper(*currLevelStr);
    *currLevelStr = base::utils::Str::trim(*currLevelStr);
    *currLevel = LevelHelper::convertFromString(currLevelStr->c_str());
    return true;
  }
  if (isConfig(*line)) {
    std::size_t assignment = line->find('=');
    *currConfigStr = line->substr(0, assignment);
    *currConfigStr = base::utils::Str::toUpper(*currConfigStr);
    *currConfigStr = base::utils::Str::trim(*currConfigStr);
    currConfig = ConfigurationTypeHelper::convertFromString(currConfigStr->c_str());
    currValue = line->substr(assignment + 1);
    currValue = base::utils::Str::trim(currValue);
    std::size_t quotesStart = currValue.find("\"", 0);
    std::size_t quotesEnd = std::string::npos;
    if (quotesStart != std::string::npos) {
      quotesEnd = currValue.find("\"", quotesStart + 1);
      while (quotesEnd != std::string::npos && currValue.at(quotesEnd - 1) == '\\') {
        currValue = currValue.erase(quotesEnd - 1, 1);
        quotesEnd = currValue.find("\"", quotesEnd + 2);
      }
    }
    if (quotesStart != std::string::npos && quotesEnd != std::string::npos) {
      // Quote provided - check and strip if valid
      ELPP_ASSERT((quotesStart < quotesEnd), "Configuration error - No ending quote found in ["
                  << currConfigStr << "]");
      ELPP_ASSERT((quotesStart + 1 != quotesEnd), "Empty configuration value for [" << currConfigStr << "]");
      if ((quotesStart != quotesEnd) && (quotesStart + 1 != quotesEnd)) {
        // Explicit check in case if assertion is disabled
        currValue = currValue.substr(quotesStart + 1, quotesEnd - 1);
      }
    }
  }
  ELPP_ASSERT(*currLevel != Level::Unknown, "Unrecognized severity level [" << *currLevelStr << "]");
  ELPP_ASSERT(currConfig != ConfigurationType::Unknown, "Unrecognized configuration [" << *currConfigStr << "]");
  if (*currLevel == Level::Unknown || currConfig == ConfigurationType::Unknown) {
    return false;  // unrecognizable level or config
  }
  conf->set(*currLevel, currConfig, currValue);
  return true;
}

void Configurations::unsafeSetIfNotExist(Level level, ConfigurationType configurationType, const std::string& value) {
  Configuration* conf = RegistryWithPred<Configuration, Configuration::Predicate>::get(level, configurationType);
  if (conf == nullptr) {
    unsafeSet(level, configurationType, value);
  }
}

void Configurations::unsafeSet(Level level, ConfigurationType configurationType, const std::string& value) {
  Configuration* conf = RegistryWithPred<Configuration, Configuration::Predicate>::get(level, configurationType);
  if (conf == nullptr) {
    registerNew(new Configuration(level, configurationType, value));
  } else {
    conf->setValue(value);
  }
  if (level == Level::Global) {
    unsafeSetGlobally(configurationType, value, false);
  }
}

void Configurations::setGlobally(ConfigurationType configurationType, const std::string& value,
                                 bool includeGlobalLevel) {
  if (includeGlobalLevel) {
    set(Level::Global, configurationType, value);
  }
  base::type::EnumType lIndex = LevelHelper::kMinValid;
  LevelHelper::forEachLevel(&lIndex, [&](void) -> bool {
    set(LevelHelper::castFromInt(lIndex), configurationType, value);
    return false;  // Do not break lambda function yet as we need to set all levels regardless
  });
}

void Configurations::unsafeSetGlobally(ConfigurationType configurationType, const std::string& value,
                                       bool includeGlobalLevel) {
  if (includeGlobalLevel) {
    unsafeSet(Level::Global, configurationType, value);
  }
  base::type::EnumType lIndex = LevelHelper::kMinValid;
  LevelHelper::forEachLevel(&lIndex, [&](void) -> bool  {
    unsafeSet(LevelHelper::castFromInt(lIndex), configurationType, value);
    return false;  // Do not break lambda function yet as we need to set all levels regardless
  });
}

// LogBuilder

void LogBuilder::convertToColoredOutput(base::type::string_t* logLine, Level level) {
  if (!m_termSupportsColor) return;
  const base::type::char_t* resetColor = ELPP_LITERAL("\x1b[0m");
  if (level == Level::Error || level == Level::Fatal)
    *logLine = ELPP_LITERAL("\x1b[31m") + *logLine + resetColor;
  else if (level == Level::Warning)
    *logLine = ELPP_LITERAL("\x1b[33m") + *logLine + resetColor;
  else if (level == Level::Debug)
    *logLine = ELPP_LITERAL("\x1b[32m") + *logLine + resetColor;
  else if (level == Level::Info)
    *logLine = ELPP_LITERAL("\x1b[36m") + *logLine + resetColor;
  else if (level == Level::Trace)
    *logLine = ELPP_LITERAL("\x1b[35m") + *logLine + resetColor;
}

// Logger

Logger::Logger(const std::string& id, base::LogStreamsReferenceMap* logStreamsReference) :
  m_id(id),
  m_typedConfigurations(nullptr),
  m_parentApplicationName(std::string()),
  m_isConfigured(false),
  m_logStreamsReference(logStreamsReference) {
  initUnflushedCount();
}

Logger::Logger(const std::string& id, const Configurations& configurations,
               base::LogStreamsReferenceMap* logStreamsReference) :
  m_id(id),
  m_typedConfigurations(nullptr),
  m_parentApplicationName(std::string()),
  m_isConfigured(false),
  m_logStreamsReference(logStreamsReference) {
  initUnflushedCount();
  configure(configurations);
}

Logger::Logger(const Logger& logger) {
  base::utils::safeDelete(m_typedConfigurations);
  m_id = logger.m_id;
  m_typedConfigurations = logger.m_typedConfigurations;
  m_parentApplicationName = logger.m_parentApplicationName;
  m_isConfigured = logger.m_isConfigured;
  m_configurations = logger.m_configurations;
  m_unflushedCount = logger.m_unflushedCount;
  m_logStreamsReference = logger.m_logStreamsReference;
}

Logger& Logger::operator=(const Logger& logger) {
  if (&logger != this) {
    base::utils::safeDelete(m_typedConfigurations);
    m_id = logger.m_id;
    m_typedConfigurations = logger.m_typedConfigurations;
    m_parentApplicationName = logger.m_parentApplicationName;
    m_isConfigured = logger.m_isConfigured;
    m_configurations = logger.m_configurations;
    m_unflushedCount = logger.m_unflushedCount;
    m_logStreamsReference = logger.m_logStreamsReference;
  }
  return *this;
}

void Logger::configure(const Configurations& configurations) {
  m_isConfigured = false;  // we set it to false in case if we fail
  initUnflushedCount();
  if (m_typedConfigurations != nullptr) {
    Configurations* c = const_cast<Configurations*>(m_typedConfigurations->configurations());
    if (c->hasConfiguration(Level::Global, ConfigurationType::Filename)) {
      flush();
    }
  }
  base::threading::ScopedLock scopedLock(lock());
  if (m_configurations != configurations) {
    m_configurations.setFromBase(const_cast<Configurations*>(&configurations));
  }
  base::utils::safeDelete(m_typedConfigurations);
  m_typedConfigurations = new base::TypedConfigurations(&m_configurations, m_logStreamsReference);
  resolveLoggerFormatSpec();
  m_isConfigured = true;
}

void Logger::reconfigure(void) {
  ELPP_INTERNAL_INFO(1, "Reconfiguring logger [" << m_id << "]");
  configure(m_configurations);
}

bool Logger::isValidId(const std::string& id) {
  for (std::string::const_iterator it = id.begin(); it != id.end(); ++it) {
    if (!base::utils::Str::contains(base::consts::kValidLoggerIdSymbols, *it)) {
      return false;
    }
  }
  return true;
}

void Logger::flush(void) {
  ELPP_INTERNAL_INFO(3, "Flushing logger [" << m_id << "] all levels");
  base::threading::ScopedLock scopedLock(lock());
  base::type::EnumType lIndex = LevelHelper::kMinValid;
  LevelHelper::forEachLevel(&lIndex, [&](void) -> bool {
    flush(LevelHelper::castFromInt(lIndex), nullptr);
    return false;
  });
}

void Logger::flush(Level level, base::type::fstream_t* fs) {
  if (fs == nullptr && m_typedConfigurations->toFile(level)) {
    fs = m_typedConfigurations->fileStream(level);
  }
  if (fs != nullptr) {
    fs->flush();
    std::unordered_map<Level, unsigned int>::iterator iter = m_unflushedCount.find(level);
    if (iter != m_unflushedCount.end()) {
      iter->second = 0;
    }
    Helpers::validateFileRolling(this, level);
  }
}

void Logger::initUnflushedCount(void) {
  m_unflushedCount.clear();
  base::type::EnumType lIndex = LevelHelper::kMinValid;
  LevelHelper::forEachLevel(&lIndex, [&](void) -> bool {
    m_unflushedCount.insert(std::make_pair(LevelHelper::castFromInt(lIndex), 0));
    return false;
  });
}

void Logger::resolveLoggerFormatSpec(void) const {
  base::type::EnumType lIndex = LevelHelper::kMinValid;
  LevelHelper::forEachLevel(&lIndex, [&](void) -> bool {
    base::LogFormat* logFormat =
    const_cast<base::LogFormat*>(&m_typedConfigurations->logFormat(LevelHelper::castFromInt(lIndex)));
    base::utils::Str::replaceFirstWithEscape(logFormat->m_format, base::consts::kLoggerIdFormatSpecifier, m_id);
    return false;
  });
}

// el::base
namespace base {

// el::base::utils
namespace utils {

// File

base::type::fstream_t* File::newFileStream(const std::string& filename) {
  base::type::fstream_t *fs = new base::type::fstream_t(filename.c_str(),
      base::type::fstream_t::out
#if !defined(ELPP_FRESH_LOG_FILE)
      | base::type::fstream_t::app
#endif
                                                       );
#if defined(ELPP_UNICODE)
  std::locale elppUnicodeLocale("");
#  if ELPP_OS_WINDOWS
  std::locale elppUnicodeLocaleWindows(elppUnicodeLocale, new std::codecvt_utf8_utf16<wchar_t>);
  elppUnicodeLocale = elppUnicodeLocaleWindows;
#  endif // ELPP_OS_WINDOWS
  fs->imbue(elppUnicodeLocale);
#endif  // defined(ELPP_UNICODE)
  if (fs->is_open()) {
    fs->flush();
  } else {
    base::utils::safeDelete(fs);
    ELPP_INTERNAL_ERROR("Bad file [" << filename << "]", true);
  }
  return fs;
}

std::size_t File::getSizeOfFile(base::type::fstream_t* fs) {
  if (fs == nullptr) {
    return 0;
  }
  // Since the file stream is appended to or truncated, the current
  // offset is the file size.
  std::size_t size = static_cast<std::size_t>(fs->tellg());
  return size;
}

bool File::pathExists(const char* path, bool considerFile) {
  if (path == nullptr) {
    return false;
  }
#if ELPP_OS_UNIX
  ELPP_UNUSED(considerFile);
  struct stat st;
  return (stat(path, &st) == 0);
#elif ELPP_OS_WINDOWS
  DWORD fileType = GetFileAttributesA(path);
  if (fileType == INVALID_FILE_ATTRIBUTES) {
    return false;
  }
  return considerFile ? true : ((fileType & FILE_ATTRIBUTE_DIRECTORY) == 0 ? false : true);
#endif  // ELPP_OS_UNIX
}

bool File::createPath(const std::string& path) {
  if (path.empty()) {
    return false;
  }
  if (base::utils::File::pathExists(path.c_str())) {
    return true;
  }
  int status = -1;

  char* currPath = const_cast<char*>(path.c_str());
  std::string builtPath = std::string();
#if ELPP_OS_UNIX
  if (path[0] == '/') {
    builtPath = "/";
  }
  currPath = STRTOK(currPath, base::consts::kFilePathSeperator, 0);
#elif ELPP_OS_WINDOWS
  // Use secure functions API
  char* nextTok_ = nullptr;
  currPath = STRTOK(currPath, base::consts::kFilePathSeperator, &nextTok_);
  ELPP_UNUSED(nextTok_);
#endif  // ELPP_OS_UNIX
  while (currPath != nullptr) {
    builtPath.append(currPath);
    builtPath.append(base::consts::kFilePathSeperator);
#if ELPP_OS_UNIX
    status = mkdir(builtPath.c_str(), ELPP_LOG_PERMS);
    currPath = STRTOK(nullptr, base::consts::kFilePathSeperator, 0);
#elif ELPP_OS_WINDOWS
    status = _mkdir(builtPath.c_str());
    currPath = STRTOK(nullptr, base::consts::kFilePathSeperator, &nextTok_);
#endif  // ELPP_OS_UNIX
  }
  if (status == -1) {
    ELPP_INTERNAL_ERROR("Error while creating path [" << path << "]", true);
    return false;
  }
  return true;
}

std::string File::extractPathFromFilename(const std::string& fullPath, const char* separator) {
  if ((fullPath == "") || (fullPath.find(separator) == std::string::npos)) {
    return fullPath;
  }
  std::size_t lastSlashAt = fullPath.find_last_of(separator);
  if (lastSlashAt == 0) {
    return std::string(separator);
  }
  return fullPath.substr(0, lastSlashAt + 1);
}

void File::buildStrippedFilename(const char* filename, char buff[], std::size_t limit) {
  std::size_t sizeOfFilename = strlen(filename);
  if (sizeOfFilename >= limit) {
    filename += (sizeOfFilename - limit);
    if (filename[0] != '.' && filename[1] != '.') {  // prepend if not already
      filename += 3;  // 3 = '..'
      STRCAT(buff, "..", limit);
    }
  }
  STRCAT(buff, filename, limit);
}

void File::buildBaseFilename(const std::string& fullPath, char buff[], std::size_t limit, const char* separator) {
  const char *filename = fullPath.c_str();
  std::size_t lastSlashAt = fullPath.find_last_of(separator);
  filename += lastSlashAt ? lastSlashAt+1 : 0;
  std::size_t sizeOfFilename = strlen(filename);
  if (sizeOfFilename >= limit) {
    filename += (sizeOfFilename - limit);
    if (filename[0] != '.' && filename[1] != '.') {  // prepend if not already
      filename += 3;  // 3 = '..'
      STRCAT(buff, "..", limit);
    }
  }
  STRCAT(buff, filename, limit);
}

// Str

bool Str::wildCardMatch(const char* str, const char* pattern) {
  while (*pattern) {
    switch (*pattern) {
    case '?':
      if (!*str)
        return false;
      ++str;
      ++pattern;
      break;
    case '*':
      if (wildCardMatch(str, pattern + 1))
        return true;
      if (*str && wildCardMatch(str + 1, pattern))
        return true;
      return false;
    default:
      if (*str++ != *pattern++)
        return false;
      break;
    }
  }
  return !*str && !*pattern;
}

std::string& Str::ltrim(std::string& str) {
  str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](char c) {
    return !std::isspace(c);
  } ));
  return str;
}

std::string& Str::rtrim(std::string& str) {
  str.erase(std::find_if(str.rbegin(), str.rend(), [](char c) {
    return !std::isspace(c);
  }).base(), str.end());
  return str;
}

std::string& Str::trim(std::string& str) {
  return ltrim(rtrim(str));
}

bool Str::startsWith(const std::string& str, const std::string& start) {
  return (str.length() >= start.length()) && (str.compare(0, start.length(), start) == 0);
}

bool Str::endsWith(const std::string& str, const std::string& end) {
  return (str.length() >= end.length()) && (str.compare(str.length() - end.length(), end.length(), end) == 0);
}

std::string& Str::replaceAll(std::string& str, char replaceWhat, char replaceWith) {
  std::replace(str.begin(), str.end(), replaceWhat, replaceWith);
  return str;
}

std::string& Str::replaceAll(std::string& str, const std::string& replaceWhat,
                             const std::string& replaceWith) {
  if (replaceWhat == replaceWith)
    return str;
  std::size_t foundAt = std::string::npos;
  while ((foundAt = str.find(replaceWhat, foundAt + 1)) != std::string::npos) {
    str.replace(foundAt, replaceWhat.length(), replaceWith);
  }
  return str;
}

void Str::replaceFirstWithEscape(base::type::string_t& str, const base::type::string_t& replaceWhat,
                                 const base::type::string_t& replaceWith) {
  std::size_t foundAt = base::type::string_t::npos;
  while ((foundAt = str.find(replaceWhat, foundAt + 1)) != base::type::string_t::npos) {
    if (foundAt > 0 && str[foundAt - 1] == base::consts::kFormatSpecifierChar) {
      str.erase(foundAt - 1, 1);
      ++foundAt;
    } else {
      str.replace(foundAt, replaceWhat.length(), replaceWith);
      return;
    }
  }
}
#if defined(ELPP_UNICODE)
void Str::replaceFirstWithEscape(base::type::string_t& str, const base::type::string_t& replaceWhat,
                                 const std::string& replaceWith) {
  replaceFirstWithEscape(str, replaceWhat, base::type::string_t(replaceWith.begin(), replaceWith.end()));
}
#endif  // defined(ELPP_UNICODE)

std::string& Str::toUpper(std::string& str) {
  std::transform(str.begin(), str.end(), str.begin(),
  [](char c) {
    return static_cast<char>(::toupper(c));
  });
  return str;
}

bool Str::cStringEq(const char* s1, const char* s2) {
  if (s1 == nullptr && s2 == nullptr) return true;
  if (s1 == nullptr || s2 == nullptr) return false;
  return strcmp(s1, s2) == 0;
}

bool Str::cStringCaseEq(const char* s1, const char* s2) {
  if (s1 == nullptr && s2 == nullptr) return true;
  if (s1 == nullptr || s2 == nullptr) return false;

  // With thanks to cygwin for this code
  int d = 0;

  while (true) {
    const int c1 = toupper(*s1++);
    const int c2 = toupper(*s2++);

    if (((d = c1 - c2) != 0) || (c2 == '\0')) {
      break;
    }
  }

  return d == 0;
}

bool Str::contains(const char* str, char c) {
  for (; *str; ++str) {
    if (*str == c)
      return true;
  }
  return false;
}

char* Str::convertAndAddToBuff(std::size_t n, int len, char* buf, const char* bufLim, bool zeroPadded) {
  char localBuff[10] = "";
  char* p = localBuff + sizeof(localBuff) - 2;
  if (n > 0) {
    for (; n > 0 && p > localBuff && len > 0; n /= 10, --len)
      *--p = static_cast<char>(n % 10 + '0');
  } else {
    *--p = '0';
    --len;
  }
  if (zeroPadded)
    while (p > localBuff && len-- > 0) *--p = static_cast<char>('0');
  return addToBuff(p, buf, bufLim);
}

char* Str::addToBuff(const char* str, char* buf, const char* bufLim) {
  while ((buf < bufLim) && ((*buf = *str++) != '\0'))
    ++buf;
  return buf;
}

char* Str::clearBuff(char buff[], std::size_t lim) {
  STRCPY(buff, "", lim);
  ELPP_UNUSED(lim);  // For *nix we dont have anything using lim in above STRCPY macro
  return buff;
}

/// @brief Converst wchar* to char*
///        NOTE: Need to free return value after use!
char* Str::wcharPtrToCharPtr(const wchar_t* line) {
  std::size_t len_ = wcslen(line) + 1;
  char* buff_ = static_cast<char*>(malloc(len_ + 1));
#      if ELPP_OS_UNIX || (ELPP_OS_WINDOWS && !ELPP_CRT_DBG_WARNINGS)
  std::wcstombs(buff_, line, len_);
#      elif ELPP_OS_WINDOWS
  std::size_t convCount_ = 0;
  mbstate_t mbState_;
  ::memset(static_cast<void*>(&mbState_), 0, sizeof(mbState_));
  wcsrtombs_s(&convCount_, buff_, len_, &line, len_, &mbState_);
#      endif  // ELPP_OS_UNIX || (ELPP_OS_WINDOWS && !ELPP_CRT_DBG_WARNINGS)
  return buff_;
}

// OS

#if ELPP_OS_WINDOWS
/// @brief Gets environment variables for Windows based OS.
///        We are not using <code>getenv(const char*)</code> because of CRT deprecation
/// @param varname Variable name to get environment variable value for
/// @return If variable exist the value of it otherwise nullptr
const char* OS::getWindowsEnvironmentVariable(const char* varname) {
  const DWORD bufferLen = 50;
  static char buffer[bufferLen];
  if (GetEnvironmentVariableA(varname, buffer, bufferLen)) {
    return buffer;
  }
  return nullptr;
}
#endif  // ELPP_OS_WINDOWS
#if ELPP_OS_ANDROID
std::string OS::getProperty(const char* prop) {
  char propVal[PROP_VALUE_MAX + 1];
  int ret = __system_property_get(prop, propVal);
  return ret == 0 ? std::string() : std::string(propVal);
}

std::string OS::getDeviceName(void) {
  std::stringstream ss;
  std::string manufacturer = getProperty("ro.product.manufacturer");
  std::string model = getProperty("ro.product.model");
  if (manufacturer.empty() || model.empty()) {
    return std::string();
  }
  ss << manufacturer << "-" << model;
  return ss.str();
}
#endif  // ELPP_OS_ANDROID

const std::string OS::getBashOutput(const char* command) {
#if (ELPP_OS_UNIX && !ELPP_OS_ANDROID && !ELPP_CYGWIN)
  if (command == nullptr) {
    return std::string();
  }
  FILE* proc = nullptr;
  if ((proc = popen(command, "r")) == nullptr) {
    ELPP_INTERNAL_ERROR("\nUnable to run command [" << command << "]", true);
    return std::string();
  }
  char hBuff[4096];
  if (fgets(hBuff, sizeof(hBuff), proc) != nullptr) {
    pclose(proc);
    const std::size_t buffLen = strlen(hBuff);
    if (buffLen > 0 && hBuff[buffLen - 1] == '\n') {
      hBuff[buffLen - 1] = '\0';
    }
    return std::string(hBuff);
  } else {
    pclose(proc);
  }
  return std::string();
#else
  ELPP_UNUSED(command);
  return std::string();
#endif  // (ELPP_OS_UNIX && !ELPP_OS_ANDROID && !ELPP_CYGWIN)
}

std::string OS::getEnvironmentVariable(const char* variableName, const char* defaultVal,
                                       const char* alternativeBashCommand) {
#if ELPP_OS_UNIX
  const char* val = getenv(variableName);
#elif ELPP_OS_WINDOWS
  const char* val = getWindowsEnvironmentVariable(variableName);
#endif  // ELPP_OS_UNIX
  if ((val == nullptr) || ((strcmp(val, "") == 0))) {
#if ELPP_OS_UNIX && defined(ELPP_FORCE_ENV_VAR_FROM_BASH)
    // Try harder on unix-based systems
    std::string valBash = base::utils::OS::getBashOutput(alternativeBashCommand);
    if (valBash.empty()) {
      return std::string(defaultVal);
    } else {
      return valBash;
    }
#elif ELPP_OS_WINDOWS || ELPP_OS_UNIX
    ELPP_UNUSED(alternativeBashCommand);
    return std::string(defaultVal);
#endif  // ELPP_OS_UNIX && defined(ELPP_FORCE_ENV_VAR_FROM_BASH)
  }
  return std::string(val);
}

std::string OS::currentUser(void) {
#if ELPP_OS_UNIX && !ELPP_OS_ANDROID
  return getEnvironmentVariable("USER", base::consts::kUnknownUser, "whoami");
#elif ELPP_OS_WINDOWS
  return getEnvironmentVariable("USERNAME", base::consts::kUnknownUser);
#elif ELPP_OS_ANDROID
  ELPP_UNUSED(base::consts::kUnknownUser);
  return std::string("android");
#else
  return std::string();
#endif  // ELPP_OS_UNIX && !ELPP_OS_ANDROID
}

std::string OS::currentHost(void) {
#if ELPP_OS_UNIX && !ELPP_OS_ANDROID
  return getEnvironmentVariable("HOSTNAME", base::consts::kUnknownHost, "hostname");
#elif ELPP_OS_WINDOWS
  return getEnvironmentVariable("COMPUTERNAME", base::consts::kUnknownHost);
#elif ELPP_OS_ANDROID
  ELPP_UNUSED(base::consts::kUnknownHost);
  return getDeviceName();
#else
  return std::string();
#endif  // ELPP_OS_UNIX && !ELPP_OS_ANDROID
}

bool OS::termSupportsColor(void) {
  std::string term = getEnvironmentVariable("TERM", "");
  return term == "xterm" || term == "xterm-color" || term == "xterm-256color"
         || term == "screen" || term == "linux" || term == "cygwin"
         || term == "screen-256color";
}

// DateTime

void DateTime::gettimeofday(struct timeval* tv) {
#if ELPP_OS_WINDOWS
  if (tv != nullptr) {
#  if ELPP_COMPILER_MSVC || defined(_MSC_EXTENSIONS)
    const unsigned __int64 delta_ = 11644473600000000Ui64;
#  else
    const unsigned __int64 delta_ = 11644473600000000ULL;
#  endif  // ELPP_COMPILER_MSVC || defined(_MSC_EXTENSIONS)
    const double secOffSet = 0.000001;
    const unsigned long usecOffSet = 1000000;
    FILETIME fileTime;
    GetSystemTimeAsFileTime(&fileTime);
    unsigned __int64 present = 0;
    present |= fileTime.dwHighDateTime;
    present = present << 32;
    present |= fileTime.dwLowDateTime;
    present /= 10;  // mic-sec
    // Subtract the difference
    present -= delta_;
    tv->tv_sec = static_cast<long>(present * secOffSet);
    tv->tv_usec = static_cast<long>(present % usecOffSet);
  }
#else
  ::gettimeofday(tv, nullptr);
#endif  // ELPP_OS_WINDOWS
}

std::string DateTime::getDateTime(const char* format, const base::SubsecondPrecision* ssPrec) {
  struct timeval currTime;
  gettimeofday(&currTime);
  return timevalToString(currTime, format, ssPrec);
}

std::string DateTime::timevalToString(struct timeval tval, const char* format,
                                      const el::base::SubsecondPrecision* ssPrec) {
  struct ::tm timeInfo;
  buildTimeInfo(&tval, &timeInfo);
  const int kBuffSize = 30;
  char buff_[kBuffSize] = "";
  parseFormat(buff_, kBuffSize, format, &timeInfo, static_cast<std::size_t>(tval.tv_usec / ssPrec->m_offset),
              ssPrec);
  return std::string(buff_);
}

base::type::string_t DateTime::formatTime(unsigned long long time, base::TimestampUnit timestampUnit) {
  base::type::EnumType start = static_cast<base::type::EnumType>(timestampUnit);
  const base::type::char_t* unit = base::consts::kTimeFormats[start].unit;
  for (base::type::EnumType i = start; i < base::consts::kTimeFormatsCount - 1; ++i) {
    if (time <= base::consts::kTimeFormats[i].value) {
      break;
    }
    if (base::consts::kTimeFormats[i].value == 1000.0f && time / 1000.0f < 1.9f) {
      break;
    }
    time /= static_cast<decltype(time)>(base::consts::kTimeFormats[i].value);
    unit = base::consts::kTimeFormats[i + 1].unit;
  }
  base::type::stringstream_t ss;
  ss << time << " " << unit;
  return ss.str();
}

unsigned long long DateTime::getTimeDifference(const struct timeval& endTime, const struct timeval& startTime,
    base::TimestampUnit timestampUnit) {
  if (timestampUnit == base::TimestampUnit::Microsecond) {
    return static_cast<unsigned long long>(static_cast<unsigned long long>(1000000 * endTime.tv_sec + endTime.tv_usec) -
                                           static_cast<unsigned long long>(1000000 * startTime.tv_sec + startTime.tv_usec));
  }
  // milliseconds
  auto conv = [](const struct timeval& tim) {
    return static_cast<unsigned long long>((tim.tv_sec * 1000) + (tim.tv_usec / 1000));
  };
  return static_cast<unsigned long long>(conv(endTime) - conv(startTime));
}

struct ::tm* DateTime::buildTimeInfo(struct timeval* currTime, struct ::tm* timeInfo) {
#if ELPP_OS_UNIX
  time_t rawTime = currTime->tv_sec;
  ::elpptime_r(&rawTime, timeInfo);
  return timeInfo;
#else
#  if ELPP_COMPILER_MSVC
  ELPP_UNUSED(currTime);
  time_t t;
#    if defined(_USE_32BIT_TIME_T)
  _time32(&t);
#    else
  _time64(&t);
#    endif
  elpptime_s(timeInfo, &t);
  return timeInfo;
#  else
  // For any other compilers that don't have CRT warnings issue e.g, MinGW or TDM GCC- we use different method
  time_t rawTime = currTime->tv_sec;
  struct tm* tmInf = elpptime(&rawTime);
  *timeInfo = *tmInf;
  return timeInfo;
#  endif  // ELPP_COMPILER_MSVC
#endif  // ELPP_OS_UNIX
}

char* DateTime::parseFormat(char* buf, std::size_t bufSz, const char* format, const struct tm* tInfo,
                            std::size_t msec, const base::SubsecondPrecision* ssPrec) {
  const char* bufLim = buf + bufSz;
  for (; *format; ++format) {
    if (*format == base::consts::kFormatSpecifierChar) {
      switch (*++format) {
      case base::consts::kFormatSpecifierChar:  // Escape
        break;
      case '\0':  // End
        --format;
        break;
      case 'd':  // Day
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_mday, 2, buf, bufLim);
        continue;
      case 'a':  // Day of week (short)
        buf = base::utils::Str::addToBuff(base::consts::kDaysAbbrev[tInfo->tm_wday], buf, bufLim);
        continue;
      case 'A':  // Day of week (long)
        buf = base::utils::Str::addToBuff(base::consts::kDays[tInfo->tm_wday], buf, bufLim);
        continue;
      case 'M':  // month
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_mon + 1, 2, buf, bufLim);
        continue;
      case 'b':  // month (short)
        buf = base::utils::Str::addToBuff(base::consts::kMonthsAbbrev[tInfo->tm_mon], buf, bufLim);
        continue;
      case 'B':  // month (long)
        buf = base::utils::Str::addToBuff(base::consts::kMonths[tInfo->tm_mon], buf, bufLim);
        continue;
      case 'y':  // year (two digits)
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_year + base::consts::kYearBase, 2, buf, bufLim);
        continue;
      case 'Y':  // year (four digits)
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_year + base::consts::kYearBase, 4, buf, bufLim);
        continue;
      case 'h':  // hour (12-hour)
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_hour % 12, 2, buf, bufLim);
        continue;
      case 'H':  // hour (24-hour)
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_hour, 2, buf, bufLim);
        continue;
      case 'm':  // minute
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_min, 2, buf, bufLim);
        continue;
      case 's':  // second
        buf = base::utils::Str::convertAndAddToBuff(tInfo->tm_sec, 2, buf, bufLim);
        continue;
      case 'z':  // subsecond part
      case 'g':
        buf = base::utils::Str::convertAndAddToBuff(msec, ssPrec->m_width, buf, bufLim);
        continue;
      case 'F':  // AM/PM
        buf = base::utils::Str::addToBuff((tInfo->tm_hour >= 12) ? base::consts::kPm : base::consts::kAm, buf, bufLim);
        continue;
      default:
        continue;
      }
    }
    if (buf == bufLim) break;
    *buf++ = *format;
  }
  return buf;
}

// CommandLineArgs

void CommandLineArgs::setArgs(int argc, char** argv) {
  m_params.clear();
  m_paramsWithValue.clear();
  if (argc == 0 || argv == nullptr) {
    return;
  }
  m_argc = argc;
  m_argv = argv;
  for (int i = 1; i < m_argc; ++i) {
    const char* v = (strstr(m_argv[i], "="));
    if (v != nullptr && strlen(v) > 0) {
      std::string key = std::string(m_argv[i]);
      key = key.substr(0, key.find_first_of('='));
      if (hasParamWithValue(key.c_str())) {
        ELPP_INTERNAL_INFO(1, "Skipping [" << key << "] arg since it already has value ["
                           << getParamValue(key.c_str()) << "]");
      } else {
        m_paramsWithValue.insert(std::make_pair(key, std::string(v + 1)));
      }
    }
    if (v == nullptr) {
      if (hasParam(m_argv[i])) {
        ELPP_INTERNAL_INFO(1, "Skipping [" << m_argv[i] << "] arg since it already exists");
      } else {
        m_params.push_back(std::string(m_argv[i]));
      }
    }
  }
}

bool CommandLineArgs::hasParamWithValue(const char* paramKey) const {
  return m_paramsWithValue.find(std::string(paramKey)) != m_paramsWithValue.end();
}

const char* CommandLineArgs::getParamValue(const char* paramKey) const {
  std::unordered_map<std::string, std::string>::const_iterator iter = m_paramsWithValue.find(std::string(paramKey));
  return iter != m_paramsWithValue.end() ? iter->second.c_str() : "";
}

bool CommandLineArgs::hasParam(const char* paramKey) const {
  return std::find(m_params.begin(), m_params.end(), std::string(paramKey)) != m_params.end();
}

bool CommandLineArgs::empty(void) const {
  return m_params.empty() && m_paramsWithValue.empty();
}

std::size_t CommandLineArgs::size(void) const {
  return m_params.size() + m_paramsWithValue.size();
}

base::type::ostream_t& operator<<(base::type::ostream_t& os, const CommandLineArgs& c) {
  for (int i = 1; i < c.m_argc; ++i) {
    os << ELPP_LITERAL("[") << c.m_argv[i] << ELPP_LITERAL("]");
    if (i < c.m_argc - 1) {
      os << ELPP_LITERAL(" ");
    }
  }
  return os;
}

} // namespace utils

// el::base::threading
namespace threading {

#if ELPP_THREADING_ENABLED
#  if ELPP_USE_STD_THREADING
#      if ELPP_ASYNC_LOGGING
static void msleep(int ms) {
  // Only when async logging enabled - this is because async is strict on compiler
#         if defined(ELPP_NO_SLEEP_FOR)
  usleep(ms * 1000);
#         else
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
#         endif  // defined(ELPP_NO_SLEEP_FOR)
}
#      endif  // ELPP_ASYNC_LOGGING
#  endif  // !ELPP_USE_STD_THREADING
#endif  // ELPP_THREADING_ENABLED

} // namespace threading

// el::base

// SubsecondPrecision

void SubsecondPrecision::init(int width) {
  if (width < 1 || width > 6) {
    width = base::consts::kDefaultSubsecondPrecision;
  }
  m_width = width;
  switch (m_width) {
  case 3:
    m_offset = 1000;
    break;
  case 4:
    m_offset = 100;
    break;
  case 5:
    m_offset = 10;
    break;
  case 6:
    m_offset = 1;
    break;
  default:
    m_offset = 1000;
    break;
  }
}

// LogFormat

LogFormat::LogFormat(void) :
  m_level(Level::Unknown),
  m_userFormat(base::type::string_t()),
  m_format(base::type::string_t()),
  m_dateTimeFormat(std::string()),
  m_flags(0x0),
  m_currentUser(base::utils::OS::currentUser()),
  m_currentHost(base::utils::OS::currentHost()) {
}

LogFormat::LogFormat(Level level, const base::type::string_t& format)
  : m_level(level), m_userFormat(format), m_currentUser(base::utils::OS::currentUser()),
    m_currentHost(base::utils::OS::currentHost()) {
  parseFromFormat(m_userFormat);
}

LogFormat::LogFormat(const LogFormat& logFormat):
  m_level(logFormat.m_level),
  m_userFormat(logFormat.m_userFormat),
  m_format(logFormat.m_format),
  m_dateTimeFormat(logFormat.m_dateTimeFormat),
  m_flags(logFormat.m_flags),
  m_currentUser(logFormat.m_currentUser),
  m_currentHost(logFormat.m_currentHost) {
}

LogFormat::LogFormat(LogFormat&& logFormat) {
  m_level = std::move(logFormat.m_level);
  m_userFormat = std::move(logFormat.m_userFormat);
  m_format = std::move(logFormat.m_format);
  m_dateTimeFormat = std::move(logFormat.m_dateTimeFormat);
  m_flags = std::move(logFormat.m_flags);
  m_currentUser = std::move(logFormat.m_currentUser);
  m_currentHost = std::move(logFormat.m_currentHost);
}

LogFormat& LogFormat::operator=(const LogFormat& logFormat) {
  if (&logFormat != this) {
    m_level = logFormat.m_level;
    m_userFormat = logFormat.m_userFormat;
    m_dateTimeFormat = logFormat.m_dateTimeFormat;
    m_flags = logFormat.m_flags;
    m_currentUser = logFormat.m_currentUser;
    m_currentHost = logFormat.m_currentHost;
  }
  return *this;
}

bool LogFormat::operator==(const LogFormat& other) {
  return m_level == other.m_level && m_userFormat == other.m_userFormat && m_format == other.m_format &&
         m_dateTimeFormat == other.m_dateTimeFormat && m_flags == other.m_flags;
}

/// @brief Updates format to be used while logging.
/// @param userFormat User provided format
void LogFormat::parseFromFormat(const base::type::string_t& userFormat) {
  // We make copy because we will be changing the format
  // i.e, removing user provided date format from original format
  // and then storing it.
  base::type::string_t formatCopy = userFormat;
  m_flags = 0x0;
  auto conditionalAddFlag = [&](const base::type::char_t* specifier, base::FormatFlags flag) {
    std::size_t foundAt = base::type::string_t::npos;
    while ((foundAt = formatCopy.find(specifier, foundAt + 1)) != base::type::string_t::npos) {
      if (foundAt > 0 && formatCopy[foundAt - 1] == base::consts::kFormatSpecifierChar) {
        if (hasFlag(flag)) {
          // If we already have flag we remove the escape chars so that '%%' is turned to '%'
          // even after specifier resolution - this is because we only replaceFirst specifier
          formatCopy.erase(foundAt - 1, 1);
          ++foundAt;
        }
      } else {
        if (!hasFlag(flag)) addFlag(flag);
      }
    }
  };
  conditionalAddFlag(base::consts::kAppNameFormatSpecifier, base::FormatFlags::AppName);
  conditionalAddFlag(base::consts::kSeverityLevelFormatSpecifier, base::FormatFlags::Level);
  conditionalAddFlag(base::consts::kSeverityLevelShortFormatSpecifier, base::FormatFlags::LevelShort);
  conditionalAddFlag(base::consts::kLoggerIdFormatSpecifier, base::FormatFlags::LoggerId);
  conditionalAddFlag(base::consts::kThreadIdFormatSpecifier, base::FormatFlags::ThreadId);
  conditionalAddFlag(base::consts::kLogFileFormatSpecifier, base::FormatFlags::File);
  conditionalAddFlag(base::consts::kLogFileBaseFormatSpecifier, base::FormatFlags::FileBase);
  conditionalAddFlag(base::consts::kLogLineFormatSpecifier, base::FormatFlags::Line);
  conditionalAddFlag(base::consts::kLogLocationFormatSpecifier, base::FormatFlags::Location);
  conditionalAddFlag(base::consts::kLogFunctionFormatSpecifier, base::FormatFlags::Function);
  conditionalAddFlag(base::consts::kCurrentUserFormatSpecifier, base::FormatFlags::User);
  conditionalAddFlag(base::consts::kCurrentHostFormatSpecifier, base::FormatFlags::Host);
  conditionalAddFlag(base::consts::kMessageFormatSpecifier, base::FormatFlags::LogMessage);
  conditionalAddFlag(base::consts::kVerboseLevelFormatSpecifier, base::FormatFlags::VerboseLevel);
  // For date/time we need to extract user's date format first
  std::size_t dateIndex = std::string::npos;
  if ((dateIndex = formatCopy.find(base::consts::kDateTimeFormatSpecifier)) != std::string::npos) {
    while (dateIndex > 0 && formatCopy[dateIndex - 1] == base::consts::kFormatSpecifierChar) {
      dateIndex = formatCopy.find(base::consts::kDateTimeFormatSpecifier, dateIndex + 1);
    }
    if (dateIndex != std::string::npos) {
      addFlag(base::FormatFlags::DateTime);
      updateDateFormat(dateIndex, formatCopy);
    }
  }
  m_format = formatCopy;
  updateFormatSpec();
}

void LogFormat::updateDateFormat(std::size_t index, base::type::string_t& currFormat) {
  if (hasFlag(base::FormatFlags::DateTime)) {
    index += ELPP_STRLEN(base::consts::kDateTimeFormatSpecifier);
  }
  const base::type::char_t* ptr = currFormat.c_str() + index;
  if ((currFormat.size() > index) && (ptr[0] == '{')) {
    // User has provided format for date/time
    ++ptr;
    int count = 1;  // Start by 1 in order to remove starting brace
    std::stringstream ss;
    for (; *ptr; ++ptr, ++count) {
      if (*ptr == '}') {
        ++count;  // In order to remove ending brace
        break;
      }
      ss << static_cast<char>(*ptr);
    }
    currFormat.erase(index, count);
    m_dateTimeFormat = ss.str();
  } else {
    // No format provided, use default
    if (hasFlag(base::FormatFlags::DateTime)) {
      m_dateTimeFormat = std::string(base::consts::kDefaultDateTimeFormat);
    }
  }
}

void LogFormat::updateFormatSpec(void) {
  // Do not use switch over strongly typed enums because Intel C++ compilers dont support them yet.
  if (m_level == Level::Debug) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kDebugLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kDebugLevelShortLogValue);
  } else if (m_level == Level::Info) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kInfoLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kInfoLevelShortLogValue);
  } else if (m_level == Level::Warning) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kWarningLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kWarningLevelShortLogValue);
  } else if (m_level == Level::Error) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kErrorLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kErrorLevelShortLogValue);
  } else if (m_level == Level::Fatal) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kFatalLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kFatalLevelShortLogValue);
  } else if (m_level == Level::Verbose) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kVerboseLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kVerboseLevelShortLogValue);
  } else if (m_level == Level::Trace) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelFormatSpecifier,
        base::consts::kTraceLevelLogValue);
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kSeverityLevelShortFormatSpecifier,
        base::consts::kTraceLevelShortLogValue);
  }
  if (hasFlag(base::FormatFlags::User)) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kCurrentUserFormatSpecifier,
        m_currentUser);
  }
  if (hasFlag(base::FormatFlags::Host)) {
    base::utils::Str::replaceFirstWithEscape(m_format, base::consts::kCurrentHostFormatSpecifier,
        m_currentHost);
  }
  // Ignore Level::Global and Level::Unknown
}

// TypedConfigurations

TypedConfigurations::TypedConfigurations(Configurations* configurations,
    base::LogStreamsReferenceMap* logStreamsReference) {
  m_configurations = configurations;
  m_logStreamsReference = logStreamsReference;
  build(m_configurations);
}

TypedConfigurations::TypedConfigurations(const TypedConfigurations& other) {
  this->m_configurations = other.m_configurations;
  this->m_logStreamsReference = other.m_logStreamsReference;
  build(m_configurations);
}

bool TypedConfigurations::enabled(Level level) {
  return getConfigByVal<bool>(level, &m_enabledMap, "enabled");
}

bool TypedConfigurations::toFile(Level level) {
  return getConfigByVal<bool>(level, &m_toFileMap, "toFile");
}

const std::string& TypedConfigurations::filename(Level level) {
  return getConfigByRef<std::string>(level, &m_filenameMap, "filename");
}

bool TypedConfigurations::toStandardOutput(Level level) {
  return getConfigByVal<bool>(level, &m_toStandardOutputMap, "toStandardOutput");
}

const base::LogFormat& TypedConfigurations::logFormat(Level level) {
  return getConfigByRef<base::LogFormat>(level, &m_logFormatMap, "logFormat");
}

const base::SubsecondPrecision& TypedConfigurations::subsecondPrecision(Level level) {
  return getConfigByRef<base::SubsecondPrecision>(level, &m_subsecondPrecisionMap, "subsecondPrecision");
}

const base::MillisecondsWidth& TypedConfigurations::millisecondsWidth(Level level) {
  return getConfigByRef<base::MillisecondsWidth>(level, &m_subsecondPrecisionMap, "millisecondsWidth");
}

bool TypedConfigurations::performanceTracking(Level level) {
  return getConfigByVal<bool>(level, &m_performanceTrackingMap, "performanceTracking");
}

base::type::fstream_t* TypedConfigurations::fileStream(Level level) {
  return getConfigByRef<base::FileStreamPtr>(level, &m_fileStreamMap, "fileStream").get();
}

std::size_t TypedConfigurations::maxLogFileSize(Level level) {
  return getConfigByVal<std::size_t>(level, &m_maxLogFileSizeMap, "maxLogFileSize");
}

std::size_t TypedConfigurations::logFlushThreshold(Level level) {
  return getConfigByVal<std::size_t>(level, &m_logFlushThresholdMap, "logFlushThreshold");
}

void TypedConfigurations::build(Configurations* configurations) {
  base::threading::ScopedLock scopedLock(lock());
  auto getBool = [] (std::string boolStr) -> bool {  // Pass by value for trimming
    base::utils::Str::trim(boolStr);
    return (boolStr == "TRUE" || boolStr == "true" || boolStr == "1");
  };
  std::vector<Configuration*> withFileSizeLimit;
  for (Configurations::const_iterator it = configurations->begin(); it != configurations->end(); ++it) {
    Configuration* conf = *it;
    // We cannot use switch on strong enums because Intel C++ dont support them yet
    if (conf->configurationType() == ConfigurationType::Enabled) {
      setValue(conf->level(), getBool(conf->value()), &m_enabledMap);
    } else if (conf->configurationType() == ConfigurationType::ToFile) {
      setValue(conf->level(), getBool(conf->value()), &m_toFileMap);
    } else if (conf->configurationType() == ConfigurationType::ToStandardOutput) {
      setValue(conf->level(), getBool(conf->value()), &m_toStandardOutputMap);
    } else if (conf->configurationType() == ConfigurationType::Filename) {
      // We do not yet configure filename but we will configure in another
      // loop. This is because if file cannot be created, we will force ToFile
      // to be false. Because configuring logger is not necessarily performance
      // sensative operation, we can live with another loop; (by the way this loop
      // is not very heavy either)
    } else if (conf->configurationType() == ConfigurationType::Format) {
      setValue(conf->level(), base::LogFormat(conf->level(),
                                              base::type::string_t(conf->value().begin(), conf->value().end())), &m_logFormatMap);
    } else if (conf->configurationType() == ConfigurationType::SubsecondPrecision) {
      setValue(Level::Global,
               base::SubsecondPrecision(static_cast<int>(getULong(conf->value()))), &m_subsecondPrecisionMap);
    } else if (conf->configurationType() == ConfigurationType::PerformanceTracking) {
      setValue(Level::Global, getBool(conf->value()), &m_performanceTrackingMap);
    } else if (conf->configurationType() == ConfigurationType::MaxLogFileSize) {
      auto v = getULong(conf->value());
      setValue(conf->level(), static_cast<std::size_t>(v), &m_maxLogFileSizeMap);
      if (v != 0) {
        withFileSizeLimit.push_back(conf);
      }
    } else if (conf->configurationType() == ConfigurationType::LogFlushThreshold) {
      setValue(conf->level(), static_cast<std::size_t>(getULong(conf->value())), &m_logFlushThresholdMap);
    }
  }
  // As mentioned earlier, we will now set filename configuration in separate loop to deal with non-existent files
  for (Configurations::const_iterator it = configurations->begin(); it != configurations->end(); ++it) {
    Configuration* conf = *it;
    if (conf->configurationType() == ConfigurationType::Filename) {
      insertFile(conf->level(), conf->value());
    }
  }
  for (std::vector<Configuration*>::iterator conf = withFileSizeLimit.begin();
       conf != withFileSizeLimit.end(); ++conf) {
    // This is not unsafe as mutex is locked in currect scope
    unsafeValidateFileRolling((*conf)->level(), base::defaultPreRollOutCallback);
  }
}

unsigned long TypedConfigurations::getULong(std::string confVal) {
  bool valid = true;
  base::utils::Str::trim(confVal);
  valid = !confVal.empty() && std::find_if(confVal.begin(), confVal.end(),
  [](char c) {
    return !base::utils::Str::isDigit(c);
  }) == confVal.end();
  if (!valid) {
    valid = false;
    ELPP_ASSERT(valid, "Configuration value not a valid integer [" << confVal << "]");
    return 0;
  }
  return atol(confVal.c_str());
}

std::string TypedConfigurations::resolveFilename(const std::string& filename) {
  std::string resultingFilename = filename;
  std::size_t dateIndex = std::string::npos;
  std::string dateTimeFormatSpecifierStr = std::string(base::consts::kDateTimeFormatSpecifierForFilename);
  if ((dateIndex = resultingFilename.find(dateTimeFormatSpecifierStr.c_str())) != std::string::npos) {
    while (dateIndex > 0 && resultingFilename[dateIndex - 1] == base::consts::kFormatSpecifierChar) {
      dateIndex = resultingFilename.find(dateTimeFormatSpecifierStr.c_str(), dateIndex + 1);
    }
    if (dateIndex != std::string::npos) {
      const char* ptr = resultingFilename.c_str() + dateIndex;
      // Goto end of specifier
      ptr += dateTimeFormatSpecifierStr.size();
      std::string fmt;
      if ((resultingFilename.size() > dateIndex) && (ptr[0] == '{')) {
        // User has provided format for date/time
        ++ptr;
        int count = 1;  // Start by 1 in order to remove starting brace
        std::stringstream ss;
        for (; *ptr; ++ptr, ++count) {
          if (*ptr == '}') {
            ++count;  // In order to remove ending brace
            break;
          }
          ss << *ptr;
        }
        resultingFilename.erase(dateIndex + dateTimeFormatSpecifierStr.size(), count);
        fmt = ss.str();
      } else {
        fmt = std::string(base::consts::kDefaultDateTimeFormatInFilename);
      }
      base::SubsecondPrecision ssPrec(3);
      std::string now = base::utils::DateTime::getDateTime(fmt.c_str(), &ssPrec);
      base::utils::Str::replaceAll(now, '/', '-'); // Replace path element since we are dealing with filename
      base::utils::Str::replaceAll(resultingFilename, dateTimeFormatSpecifierStr, now);
    }
  }
  return resultingFilename;
}

void TypedConfigurations::insertFile(Level level, const std::string& fullFilename) {
  std::string resolvedFilename = resolveFilename(fullFilename);
  if (resolvedFilename.empty()) {
    std::cerr << "Could not load empty file for logging, please re-check your configurations for level ["
              << LevelHelper::convertToString(level) << "]";
  }
  std::string filePath = base::utils::File::extractPathFromFilename(resolvedFilename, base::consts::kFilePathSeperator);
  if (filePath.size() < resolvedFilename.size()) {
    base::utils::File::createPath(filePath);
  }
  auto create = [&](Level level) {
    base::LogStreamsReferenceMap::iterator filestreamIter = m_logStreamsReference->find(resolvedFilename);
    base::type::fstream_t* fs = nullptr;
    if (filestreamIter == m_logStreamsReference->end()) {
      // We need a completely new stream, nothing to share with
      fs = base::utils::File::newFileStream(resolvedFilename);
      m_filenameMap.insert(std::make_pair(level, resolvedFilename));
      m_fileStreamMap.insert(std::make_pair(level, base::FileStreamPtr(fs)));
      m_logStreamsReference->insert(std::make_pair(resolvedFilename, base::FileStreamPtr(m_fileStreamMap.at(level))));
    } else {
      // Woops! we have an existing one, share it!
      m_filenameMap.insert(std::make_pair(level, filestreamIter->first));
      m_fileStreamMap.insert(std::make_pair(level, base::FileStreamPtr(filestreamIter->second)));
      fs = filestreamIter->second.get();
    }
    if (fs == nullptr) {
      // We display bad file error from newFileStream()
      ELPP_INTERNAL_ERROR("Setting [TO_FILE] of ["
                          << LevelHelper::convertToString(level) << "] to FALSE", false);
      setValue(level, false, &m_toFileMap);
    }
  };
  // If we dont have file conf for any level, create it for Level::Global first
  // otherwise create for specified level
  create(m_filenameMap.empty() && m_fileStreamMap.empty() ? Level::Global : level);
}

bool TypedConfigurations::unsafeValidateFileRolling(Level level, const PreRollOutCallback& preRollOutCallback) {
  base::type::fstream_t* fs = unsafeGetConfigByRef(level, &m_fileStreamMap, "fileStream").get();
  if (fs == nullptr) {
    return true;
  }
  std::size_t maxLogFileSize = unsafeGetConfigByVal(level, &m_maxLogFileSizeMap, "maxLogFileSize");
  std::size_t currFileSize = base::utils::File::getSizeOfFile(fs);
  if (maxLogFileSize != 0 && currFileSize >= maxLogFileSize) {
    std::string fname = unsafeGetConfigByRef(level, &m_filenameMap, "filename");
    ELPP_INTERNAL_INFO(1, "Truncating log file [" << fname << "] as a result of configurations for level ["
                       << LevelHelper::convertToString(level) << "]");
    fs->close();
    preRollOutCallback(fname.c_str(), currFileSize);
    fs->open(fname, std::fstream::out | std::fstream::trunc);
    return true;
  }
  return false;
}

// RegisteredHitCounters

bool RegisteredHitCounters::validateEveryN(const char* filename, base::type::LineNumber lineNumber, std::size_t n) {
  base::threading::ScopedLock scopedLock(lock());
  base::HitCounter* counter = get(filename, lineNumber);
  if (counter == nullptr) {
    registerNew(counter = new base::HitCounter(filename, lineNumber));
  }
  counter->validateHitCounts(n);
  bool result = (n >= 1 && counter->hitCounts() != 0 && counter->hitCounts() % n == 0);
  return result;
}

/// @brief Validates counter for hits >= N, i.e, registers new if does not exist otherwise updates original one
/// @return True if validation resulted in triggering hit. Meaning logs should be written everytime true is returned
bool RegisteredHitCounters::validateAfterN(const char* filename, base::type::LineNumber lineNumber, std::size_t n) {
  base::threading::ScopedLock scopedLock(lock());
  base::HitCounter* counter = get(filename, lineNumber);
  if (counter == nullptr) {
    registerNew(counter = new base::HitCounter(filename, lineNumber));
  }
  // Do not use validateHitCounts here since we do not want to reset counter here
  // Note the >= instead of > because we are incrementing
  // after this check
  if (counter->hitCounts() >= n)
    return true;
  counter->increment();
  return false;
}

/// @brief Validates counter for hits are <= n, i.e, registers new if does not exist otherwise updates original one
/// @return True if validation resulted in triggering hit. Meaning logs should be written everytime true is returned
bool RegisteredHitCounters::validateNTimes(const char* filename, base::type::LineNumber lineNumber, std::size_t n) {
  base::threading::ScopedLock scopedLock(lock());
  base::HitCounter* counter = get(filename, lineNumber);
  if (counter == nullptr) {
    registerNew(counter = new base::HitCounter(filename, lineNumber));
  }
  counter->increment();
  // Do not use validateHitCounts here since we do not want to reset counter here
  if (counter->hitCounts() <= n)
    return true;
  return false;
}

// RegisteredLoggers

RegisteredLoggers::RegisteredLoggers(const LogBuilderPtr& defaultLogBuilder) :
  m_defaultLogBuilder(defaultLogBuilder) {
  m_defaultConfigurations.setToDefault();
}

Logger* RegisteredLoggers::get(const std::string& id, bool forceCreation) {
  base::threading::ScopedLock scopedLock(lock());
  Logger* logger_ = base::utils::Registry<Logger, std::string>::get(id);
  if (logger_ == nullptr && forceCreation) {
    bool validId = Logger::isValidId(id);
    if (!validId) {
      ELPP_ASSERT(validId, "Invalid logger ID [" << id << "]. Not registering this logger.");
      return nullptr;
    }
    logger_ = new Logger(id, m_defaultConfigurations, &m_logStreamsReference);
    logger_->m_logBuilder = m_defaultLogBuilder;
    registerNew(id, logger_);
    LoggerRegistrationCallback* callback = nullptr;
    for (const std::pair<std::string, base::type::LoggerRegistrationCallbackPtr>& h
         : m_loggerRegistrationCallbacks) {
      callback = h.second.get();
      if (callback != nullptr && callback->enabled()) {
        callback->handle(logger_);
      }
    }
  }
  return logger_;
}

bool RegisteredLoggers::remove(const std::string& id) {
  if (id == base::consts::kDefaultLoggerId) {
    return false;
  }
  // get has internal lock
  Logger* logger = base::utils::Registry<Logger, std::string>::get(id);
  if (logger != nullptr) {
    // unregister has internal lock
    unregister(logger);
  }
  return true;
}

void RegisteredLoggers::unsafeFlushAll(void) {
  ELPP_INTERNAL_INFO(1, "Flushing all log files");
  for (base::LogStreamsReferenceMap::iterator it = m_logStreamsReference.begin();
       it != m_logStreamsReference.end(); ++it) {
    if (it->second.get() == nullptr) continue;
    it->second->flush();
  }
}

// VRegistry

VRegistry::VRegistry(base::type::VerboseLevel level, base::type::EnumType* pFlags) : m_level(level), m_pFlags(pFlags) {
}

/// @brief Sets verbose level. Accepted range is 0-9
void VRegistry::setLevel(base::type::VerboseLevel level) {
  base::threading::ScopedLock scopedLock(lock());
  if (level > 9)
    m_level = base::consts::kMaxVerboseLevel;
  else
    m_level = level;
}

void VRegistry::setModules(const char* modules) {
  base::threading::ScopedLock scopedLock(lock());
  auto addSuffix = [](std::stringstream& ss, const char* sfx, const char* prev) {
    if (prev != nullptr && base::utils::Str::endsWith(ss.str(), std::string(prev))) {
      std::string chr(ss.str().substr(0, ss.str().size() - strlen(prev)));
      ss.str(std::string(""));
      ss << chr;
    }
    if (base::utils::Str::endsWith(ss.str(), std::string(sfx))) {
      std::string chr(ss.str().substr(0, ss.str().size() - strlen(sfx)));
      ss.str(std::string(""));
      ss << chr;
    }
    ss << sfx;
  };
  auto insert = [&](std::stringstream& ss, base::type::VerboseLevel level) {
    if (!base::utils::hasFlag(LoggingFlag::DisableVModulesExtensions, *m_pFlags)) {
      addSuffix(ss, ".h", nullptr);
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".c", ".h");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".cpp", ".c");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".cc", ".cpp");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".cxx", ".cc");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".-inl.h", ".cxx");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".hxx", ".-inl.h");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".hpp", ".hxx");
      m_modules.insert(std::make_pair(ss.str(), level));
      addSuffix(ss, ".hh", ".hpp");
    }
    m_modules.insert(std::make_pair(ss.str(), level));
  };
  bool isMod = true;
  bool isLevel = false;
  std::stringstream ss;
  int level = -1;
  for (; *modules; ++modules) {
    switch (*modules) {
    case '=':
      isLevel = true;
      isMod = false;
      break;
    case ',':
      isLevel = false;
      isMod = true;
      if (!ss.str().empty() && level != -1) {
        insert(ss, static_cast<base::type::VerboseLevel>(level));
        ss.str(std::string(""));
        level = -1;
      }
      break;
    default:
      if (isMod) {
        ss << *modules;
      } else if (isLevel) {
        if (isdigit(*modules)) {
          level = static_cast<base::type::VerboseLevel>(*modules) - 48;
        }
      }
      break;
    }
  }
  if (!ss.str().empty() && level != -1) {
    insert(ss, static_cast<base::type::VerboseLevel>(level));
  }
}

bool VRegistry::allowed(base::type::VerboseLevel vlevel, const char* file) {
  base::threading::ScopedLock scopedLock(lock());
  if (m_modules.empty() || file == nullptr) {
    return vlevel <= m_level;
  } else {
    char baseFilename[base::consts::kSourceFilenameMaxLength] = "";
    base::utils::File::buildBaseFilename(file, baseFilename);
    std::unordered_map<std::string, base::type::VerboseLevel>::iterator it = m_modules.begin();
    for (; it != m_modules.end(); ++it) {
      if (base::utils::Str::wildCardMatch(baseFilename, it->first.c_str())) {
        return vlevel <= it->second;
      }
    }
    if (base::utils::hasFlag(LoggingFlag::AllowVerboseIfModuleNotSpecified, *m_pFlags)) {
      return true;
    }
    return false;
  }
}

void VRegistry::setFromArgs(const base::utils::CommandLineArgs* commandLineArgs) {
  if (commandLineArgs->hasParam("-v") || commandLineArgs->hasParam("--verbose") ||
      commandLineArgs->hasParam("-V") || commandLineArgs->hasParam("--VERBOSE")) {
    setLevel(base::consts::kMaxVerboseLevel);
  } else if (commandLineArgs->hasParamWithValue("--v")) {
    setLevel(static_cast<base::type::VerboseLevel>(atoi(commandLineArgs->getParamValue("--v"))));
  } else if (commandLineArgs->hasParamWithValue("--V")) {
    setLevel(static_cast<base::type::VerboseLevel>(atoi(commandLineArgs->getParamValue("--V"))));
  } else if ((commandLineArgs->hasParamWithValue("-vmodule")) && vModulesEnabled()) {
    setModules(commandLineArgs->getParamValue("-vmodule"));
  } else if (commandLineArgs->hasParamWithValue("-VMODULE") && vModulesEnabled()) {
    setModules(commandLineArgs->getParamValue("-VMODULE"));
  }
}

#if !defined(ELPP_DEFAULT_LOGGING_FLAGS)
#   define ELPP_DEFAULT_LOGGING_FLAGS 0x0
#endif // !defined(ELPP_DEFAULT_LOGGING_FLAGS)
// Storage
#if ELPP_ASYNC_LOGGING
Storage::Storage(const LogBuilderPtr& defaultLogBuilder, base::IWorker* asyncDispatchWorker) :
#else
Storage::Storage(const LogBuilderPtr& defaultLogBuilder) :
#endif  // ELPP_ASYNC_LOGGING
  m_registeredHitCounters(new base::RegisteredHitCounters()),
  m_registeredLoggers(new base::RegisteredLoggers(defaultLogBuilder)),
  m_flags(ELPP_DEFAULT_LOGGING_FLAGS),
  m_vRegistry(new base::VRegistry(0, &m_flags)),

#if ELPP_ASYNC_LOGGING
  m_asyncLogQueue(new base::AsyncLogQueue()),
  m_asyncDispatchWorker(asyncDispatchWorker),
#endif  // ELPP_ASYNC_LOGGING

  m_preRollOutCallback(base::defaultPreRollOutCallback) {
  // Register default logger
  m_registeredLoggers->get(std::string(base::consts::kDefaultLoggerId));
  // We register default logger anyway (worse case it's not going to register) just in case
  m_registeredLoggers->get("default");

#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
  // Register performance logger and reconfigure format
  Logger* performanceLogger = m_registeredLoggers->get(std::string(base::consts::kPerformanceLoggerId));
  m_registeredLoggers->get("performance");
  performanceLogger->configurations()->setGlobally(ConfigurationType::Format, std::string("%datetime %level %msg"));
  performanceLogger->reconfigure();
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)

#if defined(ELPP_SYSLOG)
  // Register syslog logger and reconfigure format
  Logger* sysLogLogger = m_registeredLoggers->get(std::string(base::consts::kSysLogLoggerId));
  sysLogLogger->configurations()->setGlobally(ConfigurationType::Format, std::string("%level: %msg"));
  sysLogLogger->reconfigure();
#endif //  defined(ELPP_SYSLOG)
  addFlag(LoggingFlag::AllowVerboseIfModuleNotSpecified);
#if ELPP_ASYNC_LOGGING
  installLogDispatchCallback<base::AsyncLogDispatchCallback>(std::string("AsyncLogDispatchCallback"));
#else
  installLogDispatchCallback<base::DefaultLogDispatchCallback>(std::string("DefaultLogDispatchCallback"));
#endif  // ELPP_ASYNC_LOGGING
#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
  installPerformanceTrackingCallback<base::DefaultPerformanceTrackingCallback>
  (std::string("DefaultPerformanceTrackingCallback"));
#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)
  ELPP_INTERNAL_INFO(1, "Easylogging++ has been initialized");
#if ELPP_ASYNC_LOGGING
  m_asyncDispatchWorker->start();
#endif  // ELPP_ASYNC_LOGGING
}

Storage::~Storage(void) {
  ELPP_INTERNAL_INFO(4, "Destroying storage");
#if ELPP_ASYNC_LOGGING
  ELPP_INTERNAL_INFO(5, "Replacing log dispatch callback to synchronous");
  uninstallLogDispatchCallback<base::AsyncLogDispatchCallback>(std::string("AsyncLogDispatchCallback"));
  installLogDispatchCallback<base::DefaultLogDispatchCallback>(std::string("DefaultLogDispatchCallback"));
  ELPP_INTERNAL_INFO(5, "Destroying asyncDispatchWorker");
  base::utils::safeDelete(m_asyncDispatchWorker);
  ELPP_INTERNAL_INFO(5, "Destroying asyncLogQueue");
  base::utils::safeDelete(m_asyncLogQueue);
#endif  // ELPP_ASYNC_LOGGING
  ELPP_INTERNAL_INFO(5, "Destroying registeredHitCounters");
  base::utils::safeDelete(m_registeredHitCounters);
  ELPP_INTERNAL_INFO(5, "Destroying registeredLoggers");
  base::utils::safeDelete(m_registeredLoggers);
  ELPP_INTERNAL_INFO(5, "Destroying vRegistry");
  base::utils::safeDelete(m_vRegistry);
}

bool Storage::hasCustomFormatSpecifier(const char* formatSpecifier) {
  base::threading::ScopedLock scopedLock(customFormatSpecifiersLock());
  return std::find(m_customFormatSpecifiers.begin(), m_customFormatSpecifiers.end(),
                   formatSpecifier) != m_customFormatSpecifiers.end();
}

void Storage::installCustomFormatSpecifier(const CustomFormatSpecifier& customFormatSpecifier) {
  if (hasCustomFormatSpecifier(customFormatSpecifier.formatSpecifier())) {
    return;
  }
  base::threading::ScopedLock scopedLock(customFormatSpecifiersLock());
  m_customFormatSpecifiers.push_back(customFormatSpecifier);
}

bool Storage::uninstallCustomFormatSpecifier(const char* formatSpecifier) {
  base::threading::ScopedLock scopedLock(customFormatSpecifiersLock());
  std::vector<CustomFormatSpecifier>::iterator it = std::find(m_customFormatSpecifiers.begin(),
      m_customFormatSpecifiers.end(), formatSpecifier);
  if (it != m_customFormatSpecifiers.end() && strcmp(formatSpecifier, it->formatSpecifier()) == 0) {
    m_customFormatSpecifiers.erase(it);
    return true;
  }
  return false;
}

void Storage::setApplicationArguments(int argc, char** argv) {
  m_commandLineArgs.setArgs(argc, argv);
  m_vRegistry->setFromArgs(commandLineArgs());
  // default log file
#if !defined(ELPP_DISABLE_LOG_FILE_FROM_ARG)
  if (m_commandLineArgs.hasParamWithValue(base::consts::kDefaultLogFileParam)) {
    Configurations c;
    c.setGlobally(ConfigurationType::Filename,
                  std::string(m_commandLineArgs.getParamValue(base::consts::kDefaultLogFileParam)));
    registeredLoggers()->setDefaultConfigurations(c);
    for (base::RegisteredLoggers::iterator it = registeredLoggers()->begin();
         it != registeredLoggers()->end(); ++it) {
      it->second->configure(c);
    }
  }
#endif  // !defined(ELPP_DISABLE_LOG_FILE_FROM_ARG)
#if defined(ELPP_LOGGING_FLAGS_FROM_ARG)
  if (m_commandLineArgs.hasParamWithValue(base::consts::kLoggingFlagsParam)) {
    int userInput = atoi(m_commandLineArgs.getParamValue(base::consts::kLoggingFlagsParam));
    if (ELPP_DEFAULT_LOGGING_FLAGS == 0x0) {
      m_flags = userInput;
    } else {
      base::utils::addFlag<base::type::EnumType>(userInput, &m_flags);
    }
  }
#endif  // defined(ELPP_LOGGING_FLAGS_FROM_ARG)
}

} // namespace base

// LogDispatchCallback
void LogDispatchCallback::handle(const LogDispatchData* data) {
#if defined(ELPP_THREAD_SAFE)
  base::threading::ScopedLock scopedLock(m_fileLocksMapLock);
  std::string filename = data->logMessage()->logger()->typedConfigurations()->filename(data->logMessage()->level());
  auto lock = m_fileLocks.find(filename);
  if (lock == m_fileLocks.end()) {
    m_fileLocks.emplace(std::make_pair(filename, std::unique_ptr<base::threading::Mutex>(new base::threading::Mutex)));
  }
#endif
}

base::threading::Mutex& LogDispatchCallback::fileHandle(const LogDispatchData* data) {
  auto it = m_fileLocks.find(data->logMessage()->logger()->typedConfigurations()->filename(data->logMessage()->level()));
  return *(it->second.get());
}

namespace base {
// DefaultLogDispatchCallback

void DefaultLogDispatchCallback::handle(const LogDispatchData* data) {
#if defined(ELPP_THREAD_SAFE)
  LogDispatchCallback::handle(data);
  base::threading::ScopedLock scopedLock(fileHandle(data));
#endif
  m_data = data;
  dispatch(m_data->logMessage()->logger()->logBuilder()->build(m_data->logMessage(),
           m_data->dispatchAction() == base::DispatchAction::NormalLog));
}

void DefaultLogDispatchCallback::dispatch(base::type::string_t&& logLine) {
  if (m_data->dispatchAction() == base::DispatchAction::NormalLog) {
    if (m_data->logMessage()->logger()->m_typedConfigurations->toFile(m_data->logMessage()->level())) {
      base::type::fstream_t* fs = m_data->logMessage()->logger()->m_typedConfigurations->fileStream(
                                    m_data->logMessage()->level());
      if (fs != nullptr) {
        fs->write(logLine.c_str(), logLine.size());
        if (fs->fail()) {
          ELPP_INTERNAL_ERROR("Unable to write log to file ["
                              << m_data->logMessage()->logger()->m_typedConfigurations->filename(m_data->logMessage()->level()) << "].\n"
                              << "Few possible reasons (could be something else):\n" << "      * Permission denied\n"
                              << "      * Disk full\n" << "      * Disk is not writable", true);
        } else {
          if (ELPP->hasFlag(LoggingFlag::ImmediateFlush)
              || (m_data->logMessage()->logger()->isFlushNeeded(m_data->logMessage()->level()))) {
            m_data->logMessage()->logger()->flush(m_data->logMessage()->level(), fs);
          }
        }
      } else {
        ELPP_INTERNAL_ERROR("Log file for [" << LevelHelper::convertToString(m_data->logMessage()->level()) << "] "
                            << "has not been configured but [TO_FILE] is configured to TRUE. [Logger ID: "
                            << m_data->logMessage()->logger()->id() << "]", false);
      }
    }
    if (m_data->logMessage()->logger()->m_typedConfigurations->toStandardOutput(m_data->logMessage()->level())) {
      if (ELPP->hasFlag(LoggingFlag::ColoredTerminalOutput))
        m_data->logMessage()->logger()->logBuilder()->convertToColoredOutput(&logLine, m_data->logMessage()->level());
      ELPP_COUT << ELPP_COUT_LINE(logLine);
    }
  }
#if defined(ELPP_SYSLOG)
  else if (m_data->dispatchAction() == base::DispatchAction::SysLog) {
    // Determine syslog priority
    int sysLogPriority = 0;
    if (m_data->logMessage()->level() == Level::Fatal)
      sysLogPriority = LOG_EMERG;
    else if (m_data->logMessage()->level() == Level::Error)
      sysLogPriority = LOG_ERR;
    else if (m_data->logMessage()->level() == Level::Warning)
      sysLogPriority = LOG_WARNING;
    else if (m_data->logMessage()->level() == Level::Info)
      sysLogPriority = LOG_INFO;
    else if (m_data->logMessage()->level() == Level::Debug)
      sysLogPriority = LOG_DEBUG;
    else
      sysLogPriority = LOG_NOTICE;
#  if defined(ELPP_UNICODE)
    char* line = base::utils::Str::wcharPtrToCharPtr(logLine.c_str());
    syslog(sysLogPriority, "%s", line);
    free(line);
#  else
    syslog(sysLogPriority, "%s", logLine.c_str());
#  endif
  }
#endif  // defined(ELPP_SYSLOG)
}

#if ELPP_ASYNC_LOGGING

// AsyncLogDispatchCallback

void AsyncLogDispatchCallback::handle(const LogDispatchData* data) {
  base::type::string_t logLine = data->logMessage()->logger()->logBuilder()->build(data->logMessage(),
                                 data->dispatchAction() == base::DispatchAction::NormalLog);
  if (data->dispatchAction() == base::DispatchAction::NormalLog
      && data->logMessage()->logger()->typedConfigurations()->toStandardOutput(data->logMessage()->level())) {
    if (ELPP->hasFlag(LoggingFlag::ColoredTerminalOutput))
      data->logMessage()->logger()->logBuilder()->convertToColoredOutput(&logLine, data->logMessage()->level());
    ELPP_COUT << ELPP_COUT_LINE(logLine);
  }
  // Save resources and only queue if we want to write to file otherwise just ignore handler
  if (data->logMessage()->logger()->typedConfigurations()->toFile(data->logMessage()->level())) {
    ELPP->asyncLogQueue()->push(AsyncLogItem(*(data->logMessage()), *data, logLine));
  }
}

// AsyncDispatchWorker
AsyncDispatchWorker::AsyncDispatchWorker() {
  setContinueRunning(false);
}

AsyncDispatchWorker::~AsyncDispatchWorker() {
  setContinueRunning(false);
  ELPP_INTERNAL_INFO(6, "Stopping dispatch worker - Cleaning log queue");
  clean();
  ELPP_INTERNAL_INFO(6, "Log queue cleaned");
}

bool AsyncDispatchWorker::clean(void) {
  std::mutex m;
  std::unique_lock<std::mutex> lk(m);
  cv.wait(lk, [] { return !ELPP->asyncLogQueue()->empty(); });
  emptyQueue();
  lk.unlock();
  cv.notify_one();
  return ELPP->asyncLogQueue()->empty();
}

void AsyncDispatchWorker::emptyQueue(void) {
  while (!ELPP->asyncLogQueue()->empty()) {
    AsyncLogItem data = ELPP->asyncLogQueue()->next();
    handle(&data);
    base::threading::msleep(100);
  }
}

void AsyncDispatchWorker::start(void) {
  base::threading::msleep(5000); // 5s (why?)
  setContinueRunning(true);
  std::thread t1(&AsyncDispatchWorker::run, this);
  t1.join();
}

void AsyncDispatchWorker::handle(AsyncLogItem* logItem) {
  LogDispatchData* data = logItem->data();
  LogMessage* logMessage = logItem->logMessage();
  Logger* logger = logMessage->logger();
  base::TypedConfigurations* conf = logger->typedConfigurations();
  base::type::string_t logLine = logItem->logLine();
  if (data->dispatchAction() == base::DispatchAction::NormalLog) {
    if (conf->toFile(logMessage->level())) {
      base::type::fstream_t* fs = conf->fileStream(logMessage->level());
      if (fs != nullptr) {
        fs->write(logLine.c_str(), logLine.size());
        if (fs->fail()) {
          ELPP_INTERNAL_ERROR("Unable to write log to file ["
                              << conf->filename(logMessage->level()) << "].\n"
                              << "Few possible reasons (could be something else):\n" << "      * Permission denied\n"
                              << "      * Disk full\n" << "      * Disk is not writable", true);
        } else {
          if (ELPP->hasFlag(LoggingFlag::ImmediateFlush) || (logger->isFlushNeeded(logMessage->level()))) {
            logger->flush(logMessage->level(), fs);
          }
        }
      } else {
        ELPP_INTERNAL_ERROR("Log file for [" << LevelHelper::convertToString(logMessage->level()) << "] "
                            << "has not been configured but [TO_FILE] is configured to TRUE. [Logger ID: " << logger->id() << "]", false);
      }
    }
  }
#  if defined(ELPP_SYSLOG)
  else if (data->dispatchAction() == base::DispatchAction::SysLog) {
    // Determine syslog priority
    int sysLogPriority = 0;
    if (logMessage->level() == Level::Fatal)
      sysLogPriority = LOG_EMERG;
    else if (logMessage->level() == Level::Error)
      sysLogPriority = LOG_ERR;
    else if (logMessage->level() == Level::Warning)
      sysLogPriority = LOG_WARNING;
    else if (logMessage->level() == Level::Info)
      sysLogPriority = LOG_INFO;
    else if (logMessage->level() == Level::Debug)
      sysLogPriority = LOG_DEBUG;
    else
      sysLogPriority = LOG_NOTICE;
#      if defined(ELPP_UNICODE)
    char* line = base::utils::Str::wcharPtrToCharPtr(logLine.c_str());
    syslog(sysLogPriority, "%s", line);
    free(line);
#      else
    syslog(sysLogPriority, "%s", logLine.c_str());
#      endif
  }
#  endif  // defined(ELPP_SYSLOG)
}

void AsyncDispatchWorker::run(void) {
  while (continueRunning()) {
    emptyQueue();
    base::threading::msleep(10); // 10ms
  }
}
#endif  // ELPP_ASYNC_LOGGING

// DefaultLogBuilder

base::type::string_t DefaultLogBuilder::build(const LogMessage* logMessage, bool appendNewLine) const {
  base::TypedConfigurations* tc = logMessage->logger()->typedConfigurations();
  const base::LogFormat* logFormat = &tc->logFormat(logMessage->level());
  base::type::string_t logLine = logFormat->format();
  char buff[base::consts::kSourceFilenameMaxLength + base::consts::kSourceLineMaxLength] = "";
  const char* bufLim = buff + sizeof(buff);
  if (logFormat->hasFlag(base::FormatFlags::AppName)) {
    // App name
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kAppNameFormatSpecifier,
        logMessage->logger()->parentApplicationName());
  }
  if (logFormat->hasFlag(base::FormatFlags::ThreadId)) {
    // Thread ID
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kThreadIdFormatSpecifier,
        ELPP->getThreadName(base::threading::getCurrentThreadId()));
  }
  if (logFormat->hasFlag(base::FormatFlags::DateTime)) {
    // DateTime
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kDateTimeFormatSpecifier,
        base::utils::DateTime::getDateTime(logFormat->dateTimeFormat().c_str(),
                                           &tc->subsecondPrecision(logMessage->level())));
  }
  if (logFormat->hasFlag(base::FormatFlags::Function)) {
    // Function
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kLogFunctionFormatSpecifier, logMessage->func());
  }
  if (logFormat->hasFlag(base::FormatFlags::File)) {
    // File
    base::utils::Str::clearBuff(buff, base::consts::kSourceFilenameMaxLength);
    base::utils::File::buildStrippedFilename(logMessage->file().c_str(), buff);
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kLogFileFormatSpecifier, std::string(buff));
  }
  if (logFormat->hasFlag(base::FormatFlags::FileBase)) {
    // FileBase
    base::utils::Str::clearBuff(buff, base::consts::kSourceFilenameMaxLength);
    base::utils::File::buildBaseFilename(logMessage->file(), buff);
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kLogFileBaseFormatSpecifier, std::string(buff));
  }
  if (logFormat->hasFlag(base::FormatFlags::Line)) {
    // Line
    char* buf = base::utils::Str::clearBuff(buff, base::consts::kSourceLineMaxLength);
    buf = base::utils::Str::convertAndAddToBuff(logMessage->line(), base::consts::kSourceLineMaxLength, buf, bufLim, false);
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kLogLineFormatSpecifier, std::string(buff));
  }
  if (logFormat->hasFlag(base::FormatFlags::Location)) {
    // Location
    char* buf = base::utils::Str::clearBuff(buff,
                                            base::consts::kSourceFilenameMaxLength + base::consts::kSourceLineMaxLength);
    base::utils::File::buildStrippedFilename(logMessage->file().c_str(), buff);
    buf = base::utils::Str::addToBuff(buff, buf, bufLim);
    buf = base::utils::Str::addToBuff(":", buf, bufLim);
    buf = base::utils::Str::convertAndAddToBuff(logMessage->line(),  base::consts::kSourceLineMaxLength, buf, bufLim,
          false);
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kLogLocationFormatSpecifier, std::string(buff));
  }
  if (logMessage->level() == Level::Verbose && logFormat->hasFlag(base::FormatFlags::VerboseLevel)) {
    // Verbose level
    char* buf = base::utils::Str::clearBuff(buff, 1);
    buf = base::utils::Str::convertAndAddToBuff(logMessage->verboseLevel(), 1, buf, bufLim, false);
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kVerboseLevelFormatSpecifier, std::string(buff));
  }
  if (logFormat->hasFlag(base::FormatFlags::LogMessage)) {
    // Log message
    base::utils::Str::replaceFirstWithEscape(logLine, base::consts::kMessageFormatSpecifier, logMessage->message());
  }
#if !defined(ELPP_DISABLE_CUSTOM_FORMAT_SPECIFIERS)
  el::base::threading::ScopedLock lock_(ELPP->customFormatSpecifiersLock());
  ELPP_UNUSED(lock_);
  for (std::vector<CustomFormatSpecifier>::const_iterator it = ELPP->customFormatSpecifiers()->begin();
       it != ELPP->customFormatSpecifiers()->end(); ++it) {
    std::string fs(it->formatSpecifier());
    base::type::string_t wcsFormatSpecifier(fs.begin(), fs.end());
    base::utils::Str::replaceFirstWithEscape(logLine, wcsFormatSpecifier, it->resolver()(logMessage));
  }
#endif  // !defined(ELPP_DISABLE_CUSTOM_FORMAT_SPECIFIERS)
  if (appendNewLine) logLine += ELPP_LITERAL("\n");
  return logLine;
}

// LogDispatcher

void LogDispatcher::dispatch(void) {
  if (m_proceed && m_dispatchAction == base::DispatchAction::None) {
    m_proceed = false;
  }
  if (!m_proceed) {
    return;
  }
#ifndef ELPP_NO_GLOBAL_LOCK
  // see https://github.com/muflihun/easyloggingpp/issues/580
  // global lock is turned off by default unless
  // ELPP_NO_GLOBAL_LOCK is defined
  base::threading::ScopedLock scopedLock(ELPP->lock());
#endif
  base::TypedConfigurations* tc = m_logMessage->logger()->m_typedConfigurations;
  if (ELPP->hasFlag(LoggingFlag::StrictLogFileSizeCheck)) {
    tc->validateFileRolling(m_logMessage->level(), ELPP->preRollOutCallback());
  }
  LogDispatchCallback* callback = nullptr;
  LogDispatchData data;
  for (const std::pair<std::string, base::type::LogDispatchCallbackPtr>& h
       : ELPP->m_logDispatchCallbacks) {
    callback = h.second.get();
    if (callback != nullptr && callback->enabled()) {
      data.setLogMessage(m_logMessage);
      data.setDispatchAction(m_dispatchAction);
      callback->handle(&data);
    }
  }
}

// MessageBuilder

void MessageBuilder::initialize(Logger* logger) {
  m_logger = logger;
  m_containerLogSeperator = ELPP->hasFlag(LoggingFlag::NewLineForContainer) ?
                            ELPP_LITERAL("\n    ") : ELPP_LITERAL(", ");
}

MessageBuilder& MessageBuilder::operator<<(const wchar_t* msg) {
  if (msg == nullptr) {
    m_logger->stream() << base::consts::kNullPointer;
    return *this;
  }
#  if defined(ELPP_UNICODE)
  m_logger->stream() << msg;
#  else
  char* buff_ = base::utils::Str::wcharPtrToCharPtr(msg);
  m_logger->stream() << buff_;
  free(buff_);
#  endif
  if (ELPP->hasFlag(LoggingFlag::AutoSpacing)) {
    m_logger->stream() << " ";
  }
  return *this;
}

// Writer

Writer& Writer::construct(Logger* logger, bool needLock) {
  m_logger = logger;
  initializeLogger(logger->id(), false, needLock);
  m_messageBuilder.initialize(m_logger);
  return *this;
}

Writer& Writer::construct(int count, const char* loggerIds, ...) {
  if (ELPP->hasFlag(LoggingFlag::MultiLoggerSupport)) {
    va_list loggersList;
    va_start(loggersList, loggerIds);
    const char* id = loggerIds;
    m_loggerIds.reserve(count);
    for (int i = 0; i < count; ++i) {
      m_loggerIds.push_back(std::string(id));
      id = va_arg(loggersList, const char*);
    }
    va_end(loggersList);
    initializeLogger(m_loggerIds.at(0));
  } else {
    initializeLogger(std::string(loggerIds));
  }
  m_messageBuilder.initialize(m_logger);
  return *this;
}

void Writer::initializeLogger(const std::string& loggerId, bool lookup, bool needLock) {
  if (lookup) {
    m_logger = ELPP->registeredLoggers()->get(loggerId, ELPP->hasFlag(LoggingFlag::CreateLoggerAutomatically));
  }
  if (m_logger == nullptr) {
    {
      if (!ELPP->registeredLoggers()->has(std::string(base::consts::kDefaultLoggerId))) {
        // Somehow default logger has been unregistered. Not good! Register again
        ELPP->registeredLoggers()->get(std::string(base::consts::kDefaultLoggerId));
      }
    }
    Writer(Level::Debug, m_file, m_line, m_func).construct(1, base::consts::kDefaultLoggerId)
        << "Logger [" << loggerId << "] is not registered yet!";
    m_proceed = false;
  } else {
    if (needLock) {
      m_logger->acquireLock();  // This should not be unlocked by checking m_proceed because
      // m_proceed can be changed by lines below
    }
    if (ELPP->hasFlag(LoggingFlag::HierarchicalLogging)) {
      m_proceed = m_level == Level::Verbose ? m_logger->enabled(m_level) :
                  LevelHelper::castToInt(m_level) >= LevelHelper::castToInt(ELPP->m_loggingLevel);
    } else {
      m_proceed = m_logger->enabled(m_level);
    }
  }
}

void Writer::processDispatch() {
#if ELPP_LOGGING_ENABLED
  if (ELPP->hasFlag(LoggingFlag::MultiLoggerSupport)) {
    bool firstDispatched = false;
    base::type::string_t logMessage;
    std::size_t i = 0;
    do {
      if (m_proceed) {
        if (firstDispatched) {
          m_logger->stream() << logMessage;
        } else {
          firstDispatched = true;
          if (m_loggerIds.size() > 1) {
            logMessage = m_logger->stream().str();
          }
        }
        triggerDispatch();
      } else if (m_logger != nullptr) {
        m_logger->stream().str(ELPP_LITERAL(""));
        m_logger->releaseLock();
      }
      if (i + 1 < m_loggerIds.size()) {
        initializeLogger(m_loggerIds.at(i + 1));
      }
    } while (++i < m_loggerIds.size());
  } else {
    if (m_proceed) {
      triggerDispatch();
    } else if (m_logger != nullptr) {
      m_logger->stream().str(ELPP_LITERAL(""));
      m_logger->releaseLock();
    }
  }
#else
  if (m_logger != nullptr) {
    m_logger->stream().str(ELPP_LITERAL(""));
    m_logger->releaseLock();
  }
#endif // ELPP_LOGGING_ENABLED
}

void Writer::triggerDispatch(void) {
  if (m_proceed) {
    if (m_msg == nullptr) {
      LogMessage msg(m_level, m_file, m_line, m_func, m_verboseLevel,
                     m_logger);
      base::LogDispatcher(m_proceed, &msg, m_dispatchAction).dispatch();
    } else {
      base::LogDispatcher(m_proceed, m_msg, m_dispatchAction).dispatch();
    }
  }
  if (m_logger != nullptr) {
    m_logger->stream().str(ELPP_LITERAL(""));
    m_logger->releaseLock();
  }
  if (m_proceed && m_level == Level::Fatal
      && !ELPP->hasFlag(LoggingFlag::DisableApplicationAbortOnFatalLog)) {
    base::Writer(Level::Warning, m_file, m_line, m_func).construct(1, base::consts::kDefaultLoggerId)
        << "Aborting application. Reason: Fatal log at [" << m_file << ":" << m_line << "]";
    std::stringstream reasonStream;
    reasonStream << "Fatal log at [" << m_file << ":" << m_line << "]"
                 << " If you wish to disable 'abort on fatal log' please use "
                 << "el::Loggers::addFlag(el::LoggingFlag::DisableApplicationAbortOnFatalLog)";
    base::utils::abort(1, reasonStream.str());
  }
  m_proceed = false;
}

// PErrorWriter

PErrorWriter::~PErrorWriter(void) {
  if (m_proceed) {
#if ELPP_COMPILER_MSVC
    char buff[256];
    strerror_s(buff, 256, errno);
    m_logger->stream() << ": " << buff << " [" << errno << "]";
#else
    m_logger->stream() << ": " << strerror(errno) << " [" << errno << "]";
#endif
  }
}

// PerformanceTracker

#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)

PerformanceTracker::PerformanceTracker(const std::string& blockName,
                                       base::TimestampUnit timestampUnit,
                                       const std::string& loggerId,
                                       bool scopedLog, Level level) :
  m_blockName(blockName), m_timestampUnit(timestampUnit), m_loggerId(loggerId), m_scopedLog(scopedLog),
  m_level(level), m_hasChecked(false), m_lastCheckpointId(std::string()), m_enabled(false) {
#if !defined(ELPP_DISABLE_PERFORMANCE_TRACKING) && ELPP_LOGGING_ENABLED
  // We store it locally so that if user happen to change configuration by the end of scope
  // or before calling checkpoint, we still depend on state of configuraton at time of construction
  el::Logger* loggerPtr = ELPP->registeredLoggers()->get(loggerId, false);
  m_enabled = loggerPtr != nullptr && loggerPtr->m_typedConfigurations->performanceTracking(m_level);
  if (m_enabled) {
    base::utils::DateTime::gettimeofday(&m_startTime);
  }
#endif  // !defined(ELPP_DISABLE_PERFORMANCE_TRACKING) && ELPP_LOGGING_ENABLED
}

PerformanceTracker::~PerformanceTracker(void) {
#if !defined(ELPP_DISABLE_PERFORMANCE_TRACKING) && ELPP_LOGGING_ENABLED
  if (m_enabled) {
    base::threading::ScopedLock scopedLock(lock());
    if (m_scopedLog) {
      base::utils::DateTime::gettimeofday(&m_endTime);
      base::type::string_t formattedTime = getFormattedTimeTaken();
      PerformanceTrackingData data(PerformanceTrackingData::DataType::Complete);
      data.init(this);
      data.m_formattedTimeTaken = formattedTime;
      PerformanceTrackingCallback* callback = nullptr;
      for (const std::pair<std::string, base::type::PerformanceTrackingCallbackPtr>& h
           : ELPP->m_performanceTrackingCallbacks) {
        callback = h.second.get();
        if (callback != nullptr && callback->enabled()) {
          callback->handle(&data);
        }
      }
    }
  }
#endif  // !defined(ELPP_DISABLE_PERFORMANCE_TRACKING)
}

void PerformanceTracker::checkpoint(const std::string& id, const char* file, base::type::LineNumber line,
                                    const char* func) {
#if !defined(ELPP_DISABLE_PERFORMANCE_TRACKING) && ELPP_LOGGING_ENABLED
  if (m_enabled) {
    base::threading::ScopedLock scopedLock(lock());
    base::utils::DateTime::gettimeofday(&m_endTime);
    base::type::string_t formattedTime = m_hasChecked ? getFormattedTimeTaken(m_lastCheckpointTime) : ELPP_LITERAL("");
    PerformanceTrackingData data(PerformanceTrackingData::DataType::Checkpoint);
    data.init(this);
    data.m_checkpointId = id;
    data.m_file = file;
    data.m_line = line;
    data.m_func = func;
    data.m_formattedTimeTaken = formattedTime;
    PerformanceTrackingCallback* callback = nullptr;
    for (const std::pair<std::string, base::type::PerformanceTrackingCallbackPtr>& h
         : ELPP->m_performanceTrackingCallbacks) {
      callback = h.second.get();
      if (callback != nullptr && callback->enabled()) {
        callback->handle(&data);
      }
    }
    base::utils::DateTime::gettimeofday(&m_lastCheckpointTime);
    m_hasChecked = true;
    m_lastCheckpointId = id;
  }
#endif  // !defined(ELPP_DISABLE_PERFORMANCE_TRACKING) && ELPP_LOGGING_ENABLED
  ELPP_UNUSED(id);
  ELPP_UNUSED(file);
  ELPP_UNUSED(line);
  ELPP_UNUSED(func);
}

const base::type::string_t PerformanceTracker::getFormattedTimeTaken(struct timeval startTime) const {
  if (ELPP->hasFlag(LoggingFlag::FixedTimeFormat)) {
    base::type::stringstream_t ss;
    ss << base::utils::DateTime::getTimeDifference(m_endTime,
        startTime, m_timestampUnit) << " " << base::consts::kTimeFormats[static_cast<base::type::EnumType>
            (m_timestampUnit)].unit;
    return ss.str();
  }
  return base::utils::DateTime::formatTime(base::utils::DateTime::getTimeDifference(m_endTime,
         startTime, m_timestampUnit), m_timestampUnit);
}

#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_PERFORMANCE_TRACKING)

namespace debug {
#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)

// StackTrace

StackTrace::StackTraceEntry::StackTraceEntry(std::size_t index, const std::string& loc, const std::string& demang,
    const std::string& hex,
    const std::string& addr) :
  m_index(index),
  m_location(loc),
  m_demangled(demang),
  m_hex(hex),
  m_addr(addr) {
}

std::ostream& operator<<(std::ostream& ss, const StackTrace::StackTraceEntry& si) {
  ss << "[" << si.m_index << "] " << si.m_location << (si.m_hex.empty() ? "" : "+") << si.m_hex << " " << si.m_addr <<
     (si.m_demangled.empty() ? "" : ":") << si.m_demangled;
  return ss;
}

std::ostream& operator<<(std::ostream& os, const StackTrace& st) {
  std::vector<StackTrace::StackTraceEntry>::const_iterator it = st.m_stack.begin();
  while (it != st.m_stack.end()) {
    os << "    " << *it++ << "\n";
  }
  return os;
}

void StackTrace::generateNew(void) {
#if ELPP_STACKTRACE
  m_stack.clear();
  void* stack[kMaxStack];
  unsigned int size = backtrace(stack, kMaxStack);
  char** strings = backtrace_symbols(stack, size);
  if (size > kStackStart) {  // Skip StackTrace c'tor and generateNew
    for (std::size_t i = kStackStart; i < size; ++i) {
      std::string mangName;
      std::string location;
      std::string hex;
      std::string addr;

      // entry: 2   crash.cpp.bin                       0x0000000101552be5 _ZN2el4base5debug10StackTraceC1Ev + 21
      const std::string line(strings[i]);
      auto p = line.find("_");
      if (p != std::string::npos) {
        mangName = line.substr(p);
        mangName = mangName.substr(0, mangName.find(" +"));
      }
      p = line.find("0x");
      if (p != std::string::npos) {
        addr = line.substr(p);
        addr = addr.substr(0, addr.find("_"));
      }
      // Perform demangling if parsed properly
      if (!mangName.empty()) {
        int status = 0;
        char* demangName = abi::__cxa_demangle(mangName.data(), 0, 0, &status);
        // if demangling is successful, output the demangled function name
        if (status == 0) {
          // Success (see http://gcc.gnu.org/onlinedocs/libstdc++/libstdc++-html-USERS-4.3/a01696.html)
          StackTraceEntry entry(i - 1, location, demangName, hex, addr);
          m_stack.push_back(entry);
        } else {
          // Not successful - we will use mangled name
          StackTraceEntry entry(i - 1, location, mangName, hex, addr);
          m_stack.push_back(entry);
        }
        free(demangName);
      } else {
        StackTraceEntry entry(i - 1, line);
        m_stack.push_back(entry);
      }
    }
  }
  free(strings);
#else
  ELPP_INTERNAL_INFO(1, "Stacktrace generation not supported for selected compiler");
#endif  // ELPP_STACKTRACE
}

// Static helper functions

static std::string crashReason(int sig) {
  std::stringstream ss;
  bool foundReason = false;
  for (int i = 0; i < base::consts::kCrashSignalsCount; ++i) {
    if (base::consts::kCrashSignals[i].numb == sig) {
      ss << "Application has crashed due to [" << base::consts::kCrashSignals[i].name << "] signal";
      if (ELPP->hasFlag(el::LoggingFlag::LogDetailedCrashReason)) {
        ss << std::endl <<
           "    " << base::consts::kCrashSignals[i].brief << std::endl <<
           "    " << base::consts::kCrashSignals[i].detail;
      }
      foundReason = true;
    }
  }
  if (!foundReason) {
    ss << "Application has crashed due to unknown signal [" << sig << "]";
  }
  return ss.str();
}
/// @brief Logs reason of crash from sig
static void logCrashReason(int sig, bool stackTraceIfAvailable, Level level, const char* logger) {
  if (sig == SIGINT && ELPP->hasFlag(el::LoggingFlag::IgnoreSigInt)) {
    return;
  }
  std::stringstream ss;
  ss << "CRASH HANDLED; ";
  ss << crashReason(sig);
#if ELPP_STACKTRACE
  if (stackTraceIfAvailable) {
    ss << std::endl << "    ======= Backtrace: =========" << std::endl << base::debug::StackTrace();
  }
#else
  ELPP_UNUSED(stackTraceIfAvailable);
#endif  // ELPP_STACKTRACE
  ELPP_WRITE_LOG(el::base::Writer, level, base::DispatchAction::NormalLog, logger) << ss.str();
}

static inline void crashAbort(int sig) {
  base::utils::abort(sig, std::string());
}

/// @brief Default application crash handler
///
/// @detail This function writes log using 'default' logger, prints stack trace for GCC based compilers and aborts program.
static inline void defaultCrashHandler(int sig) {
  base::debug::logCrashReason(sig, true, Level::Fatal, base::consts::kDefaultLoggerId);
  base::debug::crashAbort(sig);
}

// CrashHandler

CrashHandler::CrashHandler(bool useDefault) {
  if (useDefault) {
    setHandler(defaultCrashHandler);
  }
}

void CrashHandler::setHandler(const Handler& cHandler) {
  m_handler = cHandler;
#if defined(ELPP_HANDLE_SIGABRT)
  int i = 0;  // SIGABRT is at base::consts::kCrashSignals[0]
#else
  int i = 1;
#endif  // defined(ELPP_HANDLE_SIGABRT)
  for (; i < base::consts::kCrashSignalsCount; ++i) {
    m_handler = signal(base::consts::kCrashSignals[i].numb, cHandler);
  }
}

#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)
}  // namespace debug
} // namespace base

// el

// Helpers

#if defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)

void Helpers::crashAbort(int sig, const char* sourceFile, unsigned int long line) {
  std::stringstream ss;
  ss << base::debug::crashReason(sig).c_str();
  ss << " - [Called el::Helpers::crashAbort(" << sig << ")]";
  if (sourceFile != nullptr && strlen(sourceFile) > 0) {
    ss << " - Source: " << sourceFile;
    if (line > 0)
      ss << ":" << line;
    else
      ss << " (line number not specified)";
  }
  base::utils::abort(sig, ss.str());
}

void Helpers::logCrashReason(int sig, bool stackTraceIfAvailable, Level level, const char* logger) {
  el::base::debug::logCrashReason(sig, stackTraceIfAvailable, level, logger);
}

#endif // defined(ELPP_FEATURE_ALL) || defined(ELPP_FEATURE_CRASH_LOG)

// Loggers

Logger* Loggers::getLogger(const std::string& identity, bool registerIfNotAvailable) {
  return ELPP->registeredLoggers()->get(identity, registerIfNotAvailable);
}

void Loggers::setDefaultLogBuilder(el::LogBuilderPtr& logBuilderPtr) {
  ELPP->registeredLoggers()->setDefaultLogBuilder(logBuilderPtr);
}

bool Loggers::unregisterLogger(const std::string& identity) {
  return ELPP->registeredLoggers()->remove(identity);
}

bool Loggers::hasLogger(const std::string& identity) {
  return ELPP->registeredLoggers()->has(identity);
}

Logger* Loggers::reconfigureLogger(Logger* logger, const Configurations& configurations) {
  if (!logger) return nullptr;
  logger->configure(configurations);
  return logger;
}

Logger* Loggers::reconfigureLogger(const std::string& identity, const Configurations& configurations) {
  return Loggers::reconfigureLogger(Loggers::getLogger(identity), configurations);
}

Logger* Loggers::reconfigureLogger(const std::string& identity, ConfigurationType configurationType,
                                   const std::string& value) {
  Logger* logger = Loggers::getLogger(identity);
  if (logger == nullptr) {
    return nullptr;
  }
  logger->configurations()->set(Level::Global, configurationType, value);
  logger->reconfigure();
  return logger;
}

void Loggers::reconfigureAllLoggers(const Configurations& configurations) {
  for (base::RegisteredLoggers::iterator it = ELPP->registeredLoggers()->begin();
       it != ELPP->registeredLoggers()->end(); ++it) {
    Loggers::reconfigureLogger(it->second, configurations);
  }
}

void Loggers::reconfigureAllLoggers(Level level, ConfigurationType configurationType,
                                    const std::string& value) {
  for (base::RegisteredLoggers::iterator it = ELPP->registeredLoggers()->begin();
       it != ELPP->registeredLoggers()->end(); ++it) {
    Logger* logger = it->second;
    logger->configurations()->set(level, configurationType, value);
    logger->reconfigure();
  }
}

void Loggers::setDefaultConfigurations(const Configurations& configurations, bool reconfigureExistingLoggers) {
  ELPP->registeredLoggers()->setDefaultConfigurations(configurations);
  if (reconfigureExistingLoggers) {
    Loggers::reconfigureAllLoggers(configurations);
  }
}

const Configurations* Loggers::defaultConfigurations(void) {
  return ELPP->registeredLoggers()->defaultConfigurations();
}

const base::LogStreamsReferenceMap* Loggers::logStreamsReference(void) {
  return ELPP->registeredLoggers()->logStreamsReference();
}

base::TypedConfigurations Loggers::defaultTypedConfigurations(void) {
  return base::TypedConfigurations(
           ELPP->registeredLoggers()->defaultConfigurations(),
           ELPP->registeredLoggers()->logStreamsReference());
}

std::vector<std::string>* Loggers::populateAllLoggerIds(std::vector<std::string>* targetList) {
  targetList->clear();
  for (base::RegisteredLoggers::iterator it = ELPP->registeredLoggers()->list().begin();
       it != ELPP->registeredLoggers()->list().end(); ++it) {
    targetList->push_back(it->first);
  }
  return targetList;
}

void Loggers::configureFromGlobal(const char* globalConfigurationFilePath) {
  std::ifstream gcfStream(globalConfigurationFilePath, std::ifstream::in);
  ELPP_ASSERT(gcfStream.is_open(), "Unable to open global configuration file [" << globalConfigurationFilePath
              << "] for parsing.");
  std::string line = std::string();
  std::stringstream ss;
  Logger* logger = nullptr;
  auto configure = [&](void) {
    ELPP_INTERNAL_INFO(8, "Configuring logger: '" << logger->id() << "' with configurations \n" << ss.str()
                       << "\n--------------");
    Configurations c;
    c.parseFromText(ss.str());
    logger->configure(c);
  };
  while (gcfStream.good()) {
    std::getline(gcfStream, line);
    ELPP_INTERNAL_INFO(1, "Parsing line: " << line);
    base::utils::Str::trim(line);
    if (Configurations::Parser::isComment(line)) continue;
    Configurations::Parser::ignoreComments(&line);
    base::utils::Str::trim(line);
    if (line.size() > 2 && base::utils::Str::startsWith(line, std::string(base::consts::kConfigurationLoggerId))) {
      if (!ss.str().empty() && logger != nullptr) {
        configure();
      }
      ss.str(std::string(""));
      line = line.substr(2);
      base::utils::Str::trim(line);
      if (line.size() > 1) {
        ELPP_INTERNAL_INFO(1, "Getting logger: '" << line << "'");
        logger = getLogger(line);
      }
    } else {
      ss << line << "\n";
    }
  }
  if (!ss.str().empty() && logger != nullptr) {
    configure();
  }
}

bool Loggers::configureFromArg(const char* argKey) {
#if defined(ELPP_DISABLE_CONFIGURATION_FROM_PROGRAM_ARGS)
  ELPP_UNUSED(argKey);
#else
  if (!Helpers::commandLineArgs()->hasParamWithValue(argKey)) {
    return false;
  }
  configureFromGlobal(Helpers::commandLineArgs()->getParamValue(argKey));
#endif  // defined(ELPP_DISABLE_CONFIGURATION_FROM_PROGRAM_ARGS)
  return true;
}

void Loggers::flushAll(void) {
  ELPP->registeredLoggers()->flushAll();
}

void Loggers::setVerboseLevel(base::type::VerboseLevel level) {
  ELPP->vRegistry()->setLevel(level);
}

base::type::VerboseLevel Loggers::verboseLevel(void) {
  return ELPP->vRegistry()->level();
}

void Loggers::setVModules(const char* modules) {
  if (ELPP->vRegistry()->vModulesEnabled()) {
    ELPP->vRegistry()->setModules(modules);
  }
}

void Loggers::clearVModules(void) {
  ELPP->vRegistry()->clearModules();
}

// VersionInfo

const std::string VersionInfo::version(void) {
  return std::string("9.96.7");
}
/// @brief Release date of current version
const std::string VersionInfo::releaseDate(void) {
  return std::string("24-11-2018 0728hrs");
}

} // namespace el
