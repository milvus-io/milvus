#include "splitterwidget.h"
#include "ui_splitterwidget.h"
#include <QFileDialog>
#include <QStandardItemModel>
#include <QHeaderView>
#include <QDebug>
#include <QFont>
#include <QTabWidget>
#include <QProgressBar>
#include "easylogging++.h"
#include "splittercore.h"
#include "splitablefiledelegate.h"
#include "partprocessor.h"

SplitterWidget::SplitterWidget(QWidget *parent) :
    QWidget(parent),
    parent(static_cast<QTabWidget*>(parent)),
    ui(new Ui::SplitterWidget) {
    ui->setupUi(this);
    ui->listFiles->setGeometry(10, (ui->buttonChooseFiles->y() + ui->buttonChooseFiles->height()) + 10, this->parent->parentWidget()->width() - 20, this->parent->parentWidget()->height() - (ui->buttonChooseFiles->y() + 150));
    ui->buttonStartSplit->setGeometry(ui->listFiles->width() - ui->listFiles->x() - 10 - ui->buttonStartSplit->width(), ui->buttonChooseFiles->y(), ui->buttonChooseFiles->width(), ui->buttonChooseFiles->height());
    buttonRemove = new QPushButton(this);
    buttonRemove->setGeometry(ui->buttonChooseFiles->x() + ui->buttonChooseFiles->width() + 10, ui->buttonChooseFiles->y(), 30, ui->buttonChooseFiles->height());
    buttonRemove->setText("-");
    buttonRemove->setToolTip("Remove Selected Item");
    connect(buttonRemove, SIGNAL(clicked()), this, SLOT(on_buttonRemove_clicked()));
    buttonRemove->show();
    updateRemoveButtonUI();
    ui->labelFile->setGeometry(0,  this->parent->parentWidget()->height() - 150, this->parent->parentWidget()->width(), 150);
    fileModel = new QStandardItemModel(0, 6, this);
    fileModel->setHeaderData(0, Qt::Horizontal, "Filename");
    fileModel->setHeaderData(1, Qt::Horizontal, "Destination");
    fileModel->setHeaderData(2, Qt::Horizontal, "Progress");
    fileModel->setHeaderData(3, Qt::Horizontal, "Parts");
    fileModel->setHeaderData(4, Qt::Horizontal, "File Size (bytes)");
    fileModel->setHeaderData(5, Qt::Horizontal, "Size per part (bytes)");
    splitDelegate = new SplitableFileDelegate(this);
    ui->listFiles->setItemDelegate(splitDelegate);
    ui->listFiles->setModel(fileModel);
    ui->listFiles->setEditTriggers(QTreeView::AllEditTriggers);
    //TODO: make this QAbstractItemView::MultiSelection and fix on_buttonRemove_clicked()
    ui->listFiles->setSelectionMode(QAbstractItemView::SingleSelection);

    ui->labelFile->setFont(QFont("Arial",10));
    splitter = NULL;
    cancelButton = NULL;
    pauseResumeButton = NULL;
}

SplitterWidget::~SplitterWidget() {
    delete fileModel;
    fileModel = NULL;
    delete buttonRemove;
    buttonRemove = NULL;
    delete splitDelegate;
    splitDelegate = NULL;
    delete ui;
}

void SplitterWidget::updateSplitInfo(const QModelIndex &index) {
    int parts = fileModel->index(index.row(), 3).data().toInt();
    qint32 fileSize = fileModel->index(index.row(), 4).data().toDouble();
    qint32 sizeOfFilePart = fileSize / parts;
    qint32 sizeOfLastPart = sizeOfFilePart * parts == fileSize ? sizeOfFilePart : sizeOfFilePart + 1;
    fileModel->setData(fileModel->index(index.row(), 5), QString::number(sizeOfFilePart) + (sizeOfFilePart != sizeOfLastPart ? " + 1" : ""));
}

void SplitterWidget::on_buttonChooseFiles_clicked() {
    QFileDialog fileDialog(this, "Choose File(s) to Split", "", "*");
    fileDialog.setFileMode(QFileDialog::ExistingFiles);
    fileDialog.exec();
    QStringList filenames = fileDialog.selectedFiles();
    int r = fileModel->rowCount();
    Q_FOREACH (QString filePath, filenames) {
        if (filePath.trimmed() != "") {
            fileModel->insertRows(r, 1);
            QFileInfo fileinfo(filePath);
            if (fileinfo.isFile()) {
                qint32 fileSize = fileinfo.size();
                fileModel->setData(fileModel->index(r, 0), filePath);
                fileModel->setData(fileModel->index(r, 1), fileDialog.directory().absolutePath());
                fileModel->setData(fileModel->index(r, 2), 0);
                fileModel->setData(fileModel->index(r, 3), SplitterWidget::kDefaultMaxParts);
                fileModel->setData(fileModel->index(r, 4), fileSize);
                this->updateSplitInfo(fileModel->index(r, 0));
                ++r;
                VLOG(1) << "Added file to split [" << filePath << "]";
            }
        }
    }
}

void SplitterWidget::on_listFiles_doubleClicked(const QModelIndex &index) {
    if (index.column() != 3) {
        QFileDialog fileDialog(this, "Choose Split Destination","","All Files (*.*)");
        fileDialog.setFileMode(QFileDialog::DirectoryOnly);
        QString currDest = fileModel->data(index).toString();
        fileDialog.setDirectory(currDest);
        fileDialog.exec();
        //FIXME: Make sure destination changes only if dialog is accepted
        //see accepted() signal documentation for details
        if (fileDialog.selectedFiles().size() > 0) {
            fileModel->setData(fileModel->index(index.row(), 1),fileDialog.selectedFiles()[0]);
            on_listFiles_clicked(index);
        }
    }
}

void SplitterWidget::updateRemoveButtonUI(void) const {
    if (ui->listFiles->selectionModel() == NULL) {
        buttonRemove->setEnabled(false);
    } else {
        if (ui->listFiles->selectionModel()->selectedIndexes().size() > 0) {
            buttonRemove->setEnabled(true);
        } else {
            buttonRemove->setEnabled(false);
        }
    }
}

void SplitterWidget::on_listFiles_clicked(const QModelIndex &index) {
    ui->labelFile->setText(fileModel->data(fileModel->index(index.row(), 0)).toString());
    ui->labelFile->setText("Split <b>" + ui->labelFile->text() + "</b> to <b>" + fileModel->data(fileModel->index(index.row(), 1)).toString() +
                           "</b>");
    updateRemoveButtonUI();
}

void SplitterWidget::cancelSplit(void) const {
    ui->listFiles->setEnabled(true);
    ui->buttonChooseFiles->setEnabled(true);
    parent->setTabEnabled(1, true);
    ui->buttonStartSplit->show();
    splitter->cancel();
    delete cancelButton;
    delete pauseResumeButton;
}

void SplitterWidget::pauseResume(void) const {
    if (splitter->paused()) {
        pauseResumeButton->setText("Pause");
        splitter->resume();
    } else {
        pauseResumeButton->setText("Resume");
        splitter->pause();
    }
}

void SplitterWidget::on_buttonStartSplit_clicked() {
    ui->listFiles->setEnabled(false);
    ui->buttonChooseFiles->setEnabled(false);
    parent->setTabEnabled(1, false);
    ui->buttonStartSplit->hide();
    cancelButton = new QPushButton(this);
    cancelButton->setGeometry(ui->buttonStartSplit->geometry());
    cancelButton->setText("Cancel");
    cancelButton->show();
    pauseResumeButton = new QPushButton(this);
    pauseResumeButton->setGeometry(ui->buttonStartSplit->geometry());
    pauseResumeButton->move(pauseResumeButton->x() - pauseResumeButton->width() - 10, pauseResumeButton->y());
    pauseResumeButton->setText("Pause");
    pauseResumeButton->show();

    connect(cancelButton,SIGNAL(clicked()),this,SLOT(cancelSplit()));
    connect(pauseResumeButton,SIGNAL(clicked()),this,SLOT(pauseResume()));
    if (splitter) {
        delete splitter;
        splitter = 0;
    }
    startSplitting();
}

void SplitterWidget::startSplitting(void) {
    splitter = new SplitterCore(fileModel, this, this);
    connect(splitter, SIGNAL(updated(PartProcessor*)), this, SLOT(updated(PartProcessor*)));
    connect(splitter, SIGNAL(fileStarted(int, const QString&, const QString&, int)), this, SLOT(fileStarted(int, const QString&, const QString&, int)));
    connect(splitter, SIGNAL(fileCompleted(int)), this, SLOT(fileCompleted(int)));
    connect(splitter, SIGNAL(finished(PartProcessor*)), this, SLOT(finished(PartProcessor*)));
    connect(splitter, SIGNAL(started(PartProcessor*)), this, SLOT(started(PartProcessor*)));
    splitter->start();
}

void SplitterWidget::started(PartProcessor* startedPart) {
    //TODO: need this?
}

void SplitterWidget::finished(PartProcessor* finishedPart) {
    //TODO: need this?
}

void SplitterWidget::updated(PartProcessor* updatedPart) {
    progressBars.at(updatedPart->partIndex() - 1)->setValue(updatedPart->progress());
    qint32 totalProgress = 0;
    //TODO: optimize following
    for (int i = 0; i < progressBars.size(); i++) {
        totalProgress += progressBars.at(i)->value();
    }
    float progressPerc = (static_cast<float>(totalProgress) / static_cast<float>(updatedPart->originalFileSize())) * 100;
    fileModel->setData(fileModel->index(updatedPart->modelIndex().row(), 2), progressPerc);
    if (progressBars.at(updatedPart->partIndex() - 1)->isHidden())
        progressBars.at(updatedPart->partIndex() - 1)->show();
}

void SplitterWidget::fileStarted(int index, const QString& filename, const QString& destinationDir, int totalParts) {
    for (int p = 0; p < totalParts; p++) {
        int  fileSize = fileModel->index(index, 4).data().toInt();
        int sizeOfFilePart = fileSize / totalParts;
        int sizeOfLastPart = sizeOfFilePart * totalParts == fileSize ? sizeOfFilePart : sizeOfFilePart + 1;
        int size = p < totalParts ? sizeOfFilePart : sizeOfLastPart;

        QProgressBar* progressBarForPart = new QProgressBar(this);
        progressBarForPart->setMinimum(0);
        progressBarForPart->setMaximum(size);
        qDebug() << size;
        progressBarForPart->move(0, p * 20);
        progressBarForPart->setWindowTitle("Part " + QString::number(p));
        progressBars.push_back(progressBarForPart);
    }
}

void SplitterWidget::fileCompleted(int index) {
    for (int prog = 0; prog < progressBars.size(); prog++) {
        delete progressBars.at(prog);
    }
    progressBars.clear();
    fileModel->setData(fileModel->index(index, 2), 100);
}

void SplitterWidget::on_buttonRemove_clicked() {
    if (ui->listFiles->selectionModel() != NULL) {
        QModelIndexList selectedList = ui->listFiles->selectionModel()->selectedIndexes();
        QModelIndex index;
        for (int i = selectedList.size() - 1; i >= 0 ; i--) {
            index = selectedList.at(i);
            fileModel->removeRow(index.row());
            //FIXME: this is ridiculous! selectedList does not have correct number
            //of items. So it removes multiple items (till end of list) if
            //we don't break loop here.
            break;
        }
    }
    updateRemoveButtonUI();

}
