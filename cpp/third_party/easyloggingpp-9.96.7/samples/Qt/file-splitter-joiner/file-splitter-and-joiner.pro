#-------------------------------------------------
#
# Project created by QtCreator 2012-12-12T18:34:35
#
#-------------------------------------------------

QT       += core gui

greaterThan(QT_MAJOR_VERSION, 4): QT += widgets

TARGET = file-splitter-and-joiner
TEMPLATE = app

DEFINES += ELPP_QT_LOGGING    \
          ELPP_FEATURE_ALL \
          ELPP_STL_LOGGING   \
          ELPP_STRICT_SIZE_CHECK \
          ELPP_FEATURE_CRASH_LOG \
          ELPP_THREAD_SAFE

COMPILER = g++
QMAKE_CC = $$COMPILER
QMAKE_CXX = $$COMPILER
QMAKE_LINK = $$COMPILER

QMAKE_CXXFLAGS += -std=c++11

SOURCES += main.cpp\
        mainwindow.cpp \
    splitterwidget.cpp \
    joinerwidget.cpp \
    splitablefiledelegate.cpp \
    splittercore.cpp \
    partprocessor.cpp \
    addsplittedfiledialog.cpp \
    joinercore.cpp \
    about.cpp \
    ../../../src/easylogging++.cc

HEADERS  += mainwindow.h \
    easylogging++.h \
    splitterwidget.h \
    joinerwidget.h \
    splitablefiledelegate.h \
    splittercore.h \
    partprocessor.h \
    addsplittedfiledialog.h \
    joinercore.h \
    about.h

FORMS += \
    joinerwidget.ui \
    splitterwidget.ui \
    addsplittedfiledialog.ui \
    about.ui
