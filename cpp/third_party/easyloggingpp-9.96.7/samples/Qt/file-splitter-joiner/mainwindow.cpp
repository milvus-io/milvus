#include "mainwindow.h"
#include <QApplication>
#include <QDesktopWidget>
#include <QTabWidget>
#include "easylogging++.h"
#include "splitterwidget.h"
#include "joinerwidget.h"
#include "about.h"

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent) {
    this->setupUi();
}

MainWindow::~MainWindow(void) {
    delete this->splitterWidget;
    delete this->joinerWidget;
    delete this->tabWidget;
}

void MainWindow::initWidgets(void) {
    this->tabWidget = new QTabWidget(this);
    this->splitterWidget = new SplitterWidget(this->tabWidget);
    this->joinerWidget = new JoinerWidget(this->tabWidget);
    this->about = new About(this->tabWidget);
    this->setCentralWidget(this->tabWidget);
    this->tabWidget->addTab(this->splitterWidget, "File Splitter");
    this->tabWidget->addTab(this->joinerWidget, "File Joiner");
    this->tabWidget->addTab(this->about, "About");
    this->setWindowTitle("File Splitter / Joiner");
}

void MainWindow::setupUi(void) {
    //set dimensions
    this->resize(this->kWidth, this->kHeight);
    //fix size
    this->setMinimumSize(this->kWidth, this->kHeight);
    this->setMaximumSize(this->kWidth, this->kHeight);
    //move to center of desktop
    QRect r = this->geometry();
    r.moveCenter(QApplication::desktop()->availableGeometry().center());
    this->setGeometry(r);
    //initialize widgets
    this->initWidgets();
}
