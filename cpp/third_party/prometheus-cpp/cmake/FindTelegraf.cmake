find_program(Telegraf_EXECUTABLE NAMES telegraf)
mark_as_advanced(Telegraf_EXECUTABLE)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Telegraf
  FOUND_VAR Telegraf_FOUND
  REQUIRED_VARS
    Telegraf_EXECUTABLE
)
