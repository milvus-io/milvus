#-------------------------------------------------
#
# Project created by QtCreator 2013-08-17T00:52:43
#
#-------------------------------------------------

QT       -= gui

TARGET = mylib
TEMPLATE = lib

QMAKE_CXXFLAGS += -std=c++11

DEFINES += MYLIB_LIBRARY

SOURCES += mylib.cc

HEADERS += mylib.hh\
        mylib_global.hh \
    easylogging++.h

unix:!symbian {
    maemo5 {
        target.path = /opt/usr/lib
    } else {
        target.path = /usr/lib
    }
    INSTALLS += target
}
