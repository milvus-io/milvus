QT       += core gui

greaterThan(QT_MAJOR_VERSION, 4): QT += widgets

TARGET = fast-dictionary
TEMPLATE = app

COMPILER = g++
QMAKE_CC = $$COMPILER
QMAKE_CXX = $$COMPILER
QMAKE_LINK = $$COMPILER

QMAKE_CXXFLAGS += -std=c++11
DEFINES += ELPP_FEATURE_ALL \
    ELPP_MULTI_LOGGER_SUPPORT \
    ELPP_THREAD_SAFE

SOURCES += main.cc\
        mainwindow.cc \
    listwithsearch.cc \
    ../../../src/easylogging++.cc

HEADERS  += mainwindow.hh \
    listwithsearch.hh \
    ../../../src/easylogging++.h

FORMS    += mainwindow.ui

DISTFILES += \
    words.txt
