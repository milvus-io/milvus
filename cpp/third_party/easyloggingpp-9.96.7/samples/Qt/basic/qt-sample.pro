QT       += core
greaterThan(QT_MAJOR_VERSION, 4)

CONFIG += static
DEFINES += ELPP_QT_LOGGING    \
          ELPP_FEATURE_ALL \
          ELPP_STL_LOGGING   \
          ELPP_STRICT_SIZE_CHECK ELPP_UNICODE \
          ELPP_MULTI_LOGGER_SUPPORT \
          ELPP_THREAD_SAFE

TARGET = main.cpp.bin
TEMPLATE = app
QMAKE_CXXFLAGS += -std=c++11
SOURCES += main.cpp ../../../src/easylogging++.cc
HEADERS += \
           mythread.h \
    easylogging++.h

OTHER_FILES += \
    test_conf.conf
