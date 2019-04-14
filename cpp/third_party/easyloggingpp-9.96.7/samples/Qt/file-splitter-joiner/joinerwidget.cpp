#include "joinerwidget.h"
#include "ui_joinerwidget.h"
#include <QTreeWidgetItem>
#include <QDebug>
#include "addsplittedfiledialog.h"
#include "joinercore.h"
#include "partprocessor.h"
#include "easylogging++.h"

JoinerWidget::JoinerWidget(QWidget *parent) :
    QWidget(parent),
    ui(new Ui::JoinerWidget) {
    ui->setupUi(this);
    QStringList headers;
    headers << "Filename / Parts" << "Size" << "Status";
    this->ui->treeFiles->setHeaderLabels(headers);
}

JoinerWidget::~JoinerWidget() {
    delete ui;
}

void JoinerWidget::on_buttonAddParts_clicked() {
    AddSplittedFileDialog addSplittedFileDialog(ui->treeFiles, this);
    addSplittedFileDialog.exec();
}

void JoinerWidget::on_buttonUp_clicked() {
    //TODO: implement this
    //move up
}

void JoinerWidget::on_buttonDown_clicked() {
    //TODO: implement this
    //move down
}

void JoinerWidget::on_buttonStart_clicked() {
    core = new JoinerCore(ui->treeFiles);
    core->startJoining();
}

void JoinerWidget::started(PartProcessor* startedFile) {
    //TODO: Have a pointer for current QTreeWidgetItem under progress
    //to update UI
}

void JoinerWidget::finished(PartProcessor* finishedFile) {

}

void JoinerWidget::updated(PartProcessor* updatedFile) {

}
