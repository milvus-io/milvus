#include "splittercore.h"
#include <QStandardItemModel>
#include <QDebug>
#include <QFileInfo>
#include "splitterwidget.h"
#include "partprocessor.h"
#include "easylogging++.h"

SplitterCore::SplitterCore(QStandardItemModel* fileModel, QObject* parent, SplitterWidget* parentWidget)
    :QThread(parent),
      fileModel (fileModel),
      parent(parentWidget){
}

void SplitterCore::run(void) {
    QString sourceFilename;
    QString destinationDir;
    int parts;
    qint32 sizeOfFilePart;
    qint32 sizeOfLastPart;
    qint32 fileSize = 0;
    qint32 size = 0;
    qint32 offset = 0;
    PartProcessor* core;
    QFileInfo fileInfo;
    cancelled = false;
    for (int i = 0; i < fileModel->rowCount(); i++) {
        qDeleteAll(this->splitJoinCores.begin(), this->splitJoinCores.end());
        this->splitJoinCores.clear();
        if (!cancelled) {
            sourceFilename =  fileModel->data(fileModel->index(i,0)).toString();
            fileInfo.setFile(sourceFilename);
            destinationDir =  fileModel->data(fileModel->index(i,1)).toString() + "/" + fileInfo.fileName();
            parts = fileModel->data(fileModel->index(i, 3)).toInt();
            offset = 0;
            LOG(INFO) << "Splitting " << sourceFilename.toStdString() << " into " << parts << " parts to " << destinationDir.toStdString();

            fileSize = fileModel->index(i, 4).data().toDouble();
            sizeOfFilePart = fileSize / parts;
            sizeOfLastPart = sizeOfFilePart * parts == fileSize ? sizeOfFilePart : sizeOfFilePart + 1;

            emit fileStarted(i, sourceFilename, destinationDir, parts);

            for (int p = 1; p <= parts; p++) {
                offset += p == 1 ? 0 : sizeOfFilePart;
                size = p < parts ? sizeOfFilePart : sizeOfLastPart;

                QModelIndex index = fileModel->index(i, 2);
                core = new PartProcessor(sourceFilename, destinationDir, offset, size, fileSize, p, index, PartProcessor::kSplit, this);
                connect(core, SIGNAL(updated(PartProcessor*)), this, SIGNAL(updated(PartProcessor*)));
                connect(core, SIGNAL(started(PartProcessor*)), this, SIGNAL(started(PartProcessor*)));
                connect(core, SIGNAL(finished(PartProcessor*)), this, SIGNAL(finished(PartProcessor*)));

                this->splitJoinCores.push_back(core);
            }
            for (int pid = 0; pid < splitJoinCores.size(); pid++) {
                splitJoinCores.at(pid)->start();
            }
            for (int pid = 0; pid < splitJoinCores.size(); pid++) {
                splitJoinCores.at(pid)->wait();
            }
            emit fileCompleted(i);
        } else {
            qDeleteAll(this->splitJoinCores.begin(), this->splitJoinCores.end());
            this->splitJoinCores.clear();
        }
    }
    LOG(INFO) << "Finished splitting all files!";
}

void SplitterCore::pause(void) {
    for (int i = 0; i < splitJoinCores.size(); i++) {
        splitJoinCores.at(i)->pause();
    }
}

void SplitterCore::resume(void) {
    for (int i = 0; i < splitJoinCores.size(); i++) {
        splitJoinCores.at(i)->resume();
    }
}

void SplitterCore::cancel(void) {
    this->cancelled = true;
    for (int i = 0; i < splitJoinCores.size(); i++) {
        splitJoinCores.at(i)->cancel();
    }
}

bool SplitterCore::paused(void) const {
    for (int i = 0; i < splitJoinCores.size(); i++) {
        if (splitJoinCores.at(i)->paused()) return true;
    }
    return false;
}
