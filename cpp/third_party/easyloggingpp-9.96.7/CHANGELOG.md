# Change Log

## [9.96.7] - 24-11-2018
- Adds support for compiling easyloggingpp using Emscripten. This allows the library to be compiled into Javascript or WebAssembly and run in the browser while logging to the browser's Javascript console.

## [9.96.6] - 24-11-2018
- Storage constructor (indirectly) attempts to access elStorage before it's initialized (issue #660) (@Barteks2x)
- Fixed unused variable warning while build without performance logging feature (@wrgcpp)
- Updated license

## [9.96.5] - 07-09-2018
### Fixes
- Check for level enabled when using custom log message (Advanced) (issue #666)
- Ignore interruption signal crash log

## [9.96.4] - 03-04-2018
### Fixes
- Fixes seg fault with global lock (issue #580)

## [9.96.3] - 01-04-2018
### Fixes
- Demangling in GCC fixed
- `ELPP_NO_DEFAULT_LOG_FILE` now logs to null device on major platforms (windows and unix)
- Fixes unused warnings for constants

## [9.96.2] - 27-02-2018
### Updates
- Dispatcher now passes in pointer to log message instead of creating on the fly
- Introduced new constructor for `Writer` for advanced usage (see muflihun/residue)
- Use `std::unordered_map` for memory management instead of `std::map` issue #611

## [9.96.1] - 23-02-2018
### Fixes
- Two loggers writing to same file is undefined behaviour #613

## [9.96.0] - 14-02-2018
### Fixes
- Potential deadlocks in extreme edge case #609
- Respect `MaxLogFileSize` setting even when `ELPP_NO_DEFAULT_LOG_FILE` is set (@MonsieurNicolas)
- Disable log file **initially** when using `ELPP_NO_LOG_TO_FILE`, to be consistent with documentation (@rggjan)

### Updates
- `el::Storage` no longer contains locks as it should be
- Reformatted both files with `astyle`
- License text updated

### Added
- Install a pkg-config `.pc` file (@acowley)

## [9.95.4] - 10-02-2018
### Fixes
- Fix documentation (see PR#597)
- Fix buffer underflow in getBashOutput (see PR#596)

### Updates
- Added new function `Helpers::reserveCustomFormatSpecifier` (see #606)
- Made `DateTime::buildTimeInfo` public for use

## [9.95.3] - 13-10-2017
### Fixes
- Multithreading issue fixed raised from last release at log builder

## [9.95.2] - 12-06-2017
### Fixes
 - Build fix for kFreeBSD as suggested in issue #563
 - Fixed issue with deadlock on dispatch (see #571)
 - Fixed printf like logging with thread safety (see #572)

### Updates
 - Added support for AIX (thanks to @apollo13)

## [9.95.0] - 02-08-2017
### Added
 - Added NetBSD as unix [coypoop](https://github.com/muflihun/easyloggingpp/pull/548/commits)
 - Ignore `NDEBUG` or `_DEBUG` to determine whether debug logs should be enabled or not. Use `ELPP_DISABLE_DEBUG_LOGS`

### Fixes
 - Fix compile when `_USE_32_BIT_TIME_T` defined [gggin](https://github.com/muflihun/easyloggingpp/pull/542/files)
 - Fix invalid usage of safeDelete which can cause an error with valgrind [Touyote](https://github.com/muflihun/easyloggingpp/pull/544/files)
 - Add code to ensure no nullptr references [tepperly](https://github.com/muflihun/easyloggingpp/pull/512/files)

## [9.94.2] - 12-04-2017
### Added
 - CMake option to create static lib (thanks to @romariorios)
 - Ability to use UTC time using `ELPP_UTC_DATETIME` (thanks to @romariorios)
 - CMake module updated to support static lib

### Changes
 - Renamed long format specifiers to full names with padding for readbility

### Fixes
 - Fixed Android NDK build (thanks to @MoroccanMalinois)
 - Fix `ELPP_DISABLE_LOGS` not working in VS (thanks to @goloap) #365

## [9.94.1] - 25-02-2017
### Fixed
 - Fixes for `/W4` level warnings generated in MSVC compile (Thanks to [Falconne](https://github.com/Falconne))
 - Fixed links
 - Fixes removing default logger if other than `default`

### Changes
 - Changed documentation to mention `easylogging++.cc` in introduction and added links to features

## [9.94.0] - 14-02-2017
### Fixed
 - Fixed performance tracking time unit and calculations

### Added
 - Restored `ELPP_DEFAULT_LOGGER` and `ELPP_DEFAULT_PERFORMANCE_LOGGER`
 - `Helpers::getThreadName` for reading current thread name
 - Custom format specifier now has to return `std::string` instead
 - Merged `thread_name` with `thread` if thread name is available it's used otherwise ID is displayed

For older versions please refer to [https://github.com/muflihun/easyloggingpp/tree/master/doc](https://github.com/muflihun/easyloggingpp/tree/master/doc)
