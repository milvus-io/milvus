add_subdirectory( gtest )
get_target_property( GTEST_INCLUDE_DIR gtest INTERFACE_INCLUDE_DIRECTORIES )

# INCLUDE DIRS need to be set at parent coupe.
set( THIRDPARTY_INCLUDE_DIRS
    ${GTEST_INCLUDE_DIR} PARENT_SCOPE)

