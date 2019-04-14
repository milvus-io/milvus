#include "addsplittedfiledialog.h"
#include "ui_addsplittedfiledialog.h"
#include <QFileDialog>
#include <QTreeWidgetItem>
#include <QMessageBox>
#include <QListWidgetItem>
#include <QDialogButtonBox>
#include <QFileInfo>
#include <QDebug>
#include "partprocessor.h"
#include "easylogging++.h"

AddSplittedFileDialog::AddSplittedFileDialog(QTreeWidget* parentView, QWidget *parent) :
    QDialog(parent),
    filePartsPointers(NULL),
    parentView(parentView),
    ui(new Ui::AddSplittedFileDialog) {
    ui->setupUi(this);
    buttonOkCancel = new QDialogButtonBox(this);
    buttonOkCancel->setObjectName(QString::fromUtf8("buttonBox"));
    buttonOkCancel->setGeometry(QRect(10, 200, 531, 32));
    buttonOkCancel->setOrientation(Qt::Horizontal);
    buttonOkCancel->setStandardButtons(QDialogButtonBox::Cancel|QDialogButtonBox::Ok);
    QObject::connect(buttonOkCancel, SIGNAL(accepted()), this, SLOT(on_buttonBox_accepted()));
    QObject::connect(buttonOkCancel, SIGNAL(rejected()), this, SLOT(reject()));

    filenamePointer = parentView->selectedItems().size() > 0 ? parentView->selectedItems()[0] : NULL;
    if (filenamePointer && filenamePointer->parent() != NULL) {
        filenamePointer = filenamePointer->parent();
    }
    if (filenamePointer) {
        ui->textFilename->setText(filenamePointer->text(0));
        for (int i = 0; i < filenamePointer->childCount(); i++) {
            filePartsPointers = new QTreeWidgetItem(ui->treeParts,
                                                    QStringList()
                                                    << filenamePointer->child(i)->text(0)
                                                    << filenamePointer->child(i)->text(1)
                                                    << filenamePointer->child(i)->text(2));
        }
    }
    ui->treeParts->setSelectionMode(QTreeWidget::MultiSelection);
    this->setWindowTitle("Add Part(s)");
}

AddSplittedFileDialog::~AddSplittedFileDialog() {
    for (int i = ui->treeParts->topLevelItemCount(); i >= 0; i--) {
        delete ui->treeParts->topLevelItem(i);
    }
    delete buttonOkCancel;
    delete ui;
}

void AddSplittedFileDialog::on_buttonAddPart_clicked() {
    QFileDialog fileDialog(this, "Choose File(s) to Join", "", "*" + QString(PartProcessor::kPartSuffix) + "*");
    fileDialog.setFileMode(QFileDialog::ExistingFiles);
    fileDialog.exec();
    QStringList filenames = fileDialog.selectedFiles();
    QFileInfo fileInfo;
    QString fileSize = "0";
    Q_FOREACH (QString filename, filenames) {
        fileInfo.setFile(filename);
        fileSize = QString::number(fileInfo.size());
        filePartsPointers = new QTreeWidgetItem(ui->treeParts, QStringList() << filename << fileSize << "Queued");
        VLOG(1) << "Added part [" << filename << "]";
    }
}

void AddSplittedFileDialog::on_buttonBox_accepted() {
    if (ui->treeParts->topLevelItemCount() > 0) {
        if (ui->textFilename->text() == "") {
            QMessageBox messageBox(QMessageBox::Warning, "Filename", "Please enter filename", QMessageBox::NoButton, this);
            messageBox.exec();
            ui->textFilename->setFocus();
            return;
        }
        if (!filenamePointer) {
            filenamePointer = new QTreeWidgetItem(parentView);
        } else {
            for (int i = filenamePointer->childCount(); i >= 0; i--) {
                delete filenamePointer->child(i);
            }
            delete filenamePointer;
            filenamePointer = new QTreeWidgetItem(parentView);
        }
        double totalSize = 0;
        for (int i = 0; i < ui->treeParts->topLevelItemCount(); i++) {
            filePartsPointers = ui->treeParts->topLevelItem(i);
            filePartsPointers = new QTreeWidgetItem(filenamePointer,
                                                    QStringList()
                                                    << filePartsPointers->text(0)
                                                    << filePartsPointers->text(1)
                                                    << filePartsPointers->text(2));
            totalSize += filePartsPointers->text(1).toDouble();
        }
        filenamePointer->setText(0, ui->textFilename->text());
        filenamePointer->setText(1, QString::number(totalSize));
        filenamePointer->setText(2, "Queued");
        this->accept();
    }

}

void AddSplittedFileDialog::on_pushButton_clicked() {
    QFileDialog fileDialog(this, "Choose Destination Filename","","All Files (*.*)");
    fileDialog.setFileMode(QFileDialog::AnyFile);
    fileDialog.setAcceptMode(QFileDialog::AcceptSave);
    fileDialog.setDirectory(ui->textFilename->text());
    fileDialog.exec();
    if (fileDialog.selectedFiles().size() > 0) {
        ui->textFilename->setText(fileDialog.selectedFiles()[0]);
    }
}

void AddSplittedFileDialog::on_buttonRemove_clicked() {
    qDeleteAll(ui->treeParts->selectedItems());
}
