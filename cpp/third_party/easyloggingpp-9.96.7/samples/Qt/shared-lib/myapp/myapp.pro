#-------------------------------------------------
#
# Project created by QtCreator 2013-08-17T01:02:59
#
#-------------------------------------------------

QT       += core gui

greaterThan(QT_MAJOR_VERSION, 4): QT += widgets

TARGET = myapp
TEMPLATE = app

QMAKE_CXXFLAGS += -std=c++11

DEPENDPATH += . ../mylib
INCLUDEPATH +=  ../mylib
LIBS+=  -L../build-mylib -lmylib # try in lib build dir "sudo cp %{buildDir}/libmylib.so* /usr/lib/" to make this work


SOURCES += main.cc

HEADERS  += \
    easylogging++.h

FORMS    +=
