if ( BUILD_UNIT_TEST )
    set(GTEST_LIBS gtest gtest_main gmock gmock_main)

    create_executable (
            TARGET      TimezoneTest
            SRCS        TimezoneTest
            LIBS        server ${GTEST_LIBS}
            DEFS        FIU_ENABLE
    )

    add_test ( NAME     TimezoneTest
            COMMAND  $<TARGET_FILE:TimezoneTest>
            )
endif()
