#include "partprocessor.h"
#include <QFile>
#include <QDebug>
#include <QTimer>
#include <QModelIndex>
#include "easylogging++.h"

const char* PartProcessor::kPartSuffix = "_split-part_";

PartProcessor::PartProcessor(const QString& source, const QString& destination, qint32 startOffset, qint32 maxPerPart, qint32 originalFileSize_, int part, const QModelIndex& index, kProcessType processType, QObject *parent) :
    QThread(parent),
    processType(processType),
    sourceFilename_(source),
    destinationFilename_(destination),
    seekLocation_(startOffset),
    maxSizePerPart_(maxPerPart),
    originalFileSize_(originalFileSize_),
    partIndex_(part),
    sourceFile_(new QFile(source)),
    modelIndex_(const_cast<QModelIndex&>(index)) {

    this->openFiles();
    this->cancelled_ = false;
    this->resume();
}

PartProcessor::PartProcessor(const QList<QString>& parts, const QString& destination, const QModelIndex& index, kProcessType processType, QObject *parent) :
    parts_(parts),
    processType(processType),
    destinationFilename_(destination),
    modelIndex_(const_cast<QModelIndex&>(index)),
    QThread(parent) {
    sourceFile_ = NULL;
    this->cancelled_ = false;
    this->resume();
    this->openFiles();
}

PartProcessor::~PartProcessor(void) {
    closeFiles();
    qDeleteAll(errors_);
}

void PartProcessor::closeFiles(void) {
    if (sourceFile_ && sourceFile_->isOpen()) {
        sourceFile_->close();
    }
    if (destinationFile_.isOpen()) {
        destinationFile_.close();
    }
    if (sourceFile_ != NULL) {
        delete sourceFile_;
        sourceFile_ = NULL;
    }
}

void PartProcessor::openFiles(void) {
    filesOpened = false;
    if(processType == kSplit && !sourceFile_->open(QIODevice::ReadOnly)) {
        errors_.push_back(new Error("Error opening source file", kOpenFileError));
        LOG(ERROR) << "Error opening source file";
        return;
    }
    destinationFile_.setFileName(this->destinationFilename_ + (processType == kSplit ?
                                                                   (QString(PartProcessor::kPartSuffix) + QString::number(partIndex())) : ""));
    if(!destinationFile_.open(QIODevice::WriteOnly)) {
        errors_.push_back(new Error("Error opening source file", kOpenFileError));
        LOG(ERROR) << "Error opening destination file";
        return;
    }
    filesOpened = true;
}

int PartProcessor::split(void) {
    mutex_.lock();
    LOG(DEBUG) << "Splitting " << this->sourceFilename_.toStdString() << " to " << this->destinationFilename_.toStdString();
    int nextBuffLength = PartProcessor::kBufferSize;
    char *data = new char[nextBuffLength];
    qint32 dataBytes;
    progress_ = 0;
    this->sourceFile_->seek(this->seekLocation_);
    while (!this->sourceFile_->atEnd()) {
        if (cancelled_) {
            break;
        }
        while (paused()) {
            msleep(500);
        }
        if (data == NULL) {
            break;
        }

        nextBuffLength = (progress() + PartProcessor::kBufferSize > this->maxSizePerPart_) ?
                             this->maxSizePerPart_ - progress() :
                             PartProcessor::kBufferSize;

        dataBytes = this->sourceFile_->read(data, nextBuffLength);
        this->destinationFile_.write(data, dataBytes);
        this->destinationFile_.flush();
        progress_ += nextBuffLength;
        VLOG(2) << "Progress (split) = " << progress_ << " bytes";
        if (progress() % (PartProcessor::kBufferSize * PartProcessor::kUpdateFrequencyBytes) == 0) {
            emit updated(this);
        }

        if (progress() == this->maxSizePerPart_) {
            emit updated(this);
            emit finished(this);
            break;
        }
    }
    delete[] data;
    data = NULL;
    closeFiles();
    int status = (progress() != this->maxSizePerPart_);
    mutex_.unlock();
    return status;
}

int PartProcessor::merge(void) {
    int status = -1;
    this->progress_ = 0;
    this->partIndex_ = 0;
    int progBytes = 0;
    mutex_.lock();
    for (int i = 0; i < this->parts_.size(); i++) {
        this->partIndex_ = i;
        this->progress_ = i;
        LOG(INFO) << "Merging data from: " << this->parts_.at(i).toStdString();
        //TODO: check for source file availability
        this->sourceFile_ = new QFile(this->parts_.at(i));
        if (!this->sourceFile_->open(QFile::ReadOnly)) {
            LOG(ERROR) << "Error opening files!";
            return status;
        }
        char* data = new char[PartProcessor::kBufferSize];
        qint32 dataBytes;
        while (!this->sourceFile_->atEnd()) {
            if (cancelled_) {
                break;
            }
            while (paused()) {
                msleep(500);
            }
            //TODO: check for destination file writable permissions beforehand
            dataBytes = this->sourceFile_->read(data, PartProcessor::kBufferSize);
            progBytes += static_cast<int>(dataBytes);
            VLOG(2) << "Progress (merge) = " << progBytes << " bytes";
            this->destinationFile_.write(data, dataBytes);
            this->destinationFile_.flush();
        }
        delete[] data;
        data = NULL;
        this->sourceFile_->close();
        delete this->sourceFile_;
        this->sourceFile_ = NULL;
        emit updated(this);
    }
    mutex_.unlock();
    status = this->progress_ == this->parts_.size();
    closeFiles();
    return status;
}

bool PartProcessor::paused(void) const {
    return paused_;
}

QModelIndex& PartProcessor::modelIndex(void) const {
    return modelIndex_;
}

qint32 PartProcessor::progress(void) const {
    return progress_;
}

qint32 PartProcessor::originalFileSize(void) const {
    return originalFileSize_;
}

int PartProcessor::partIndex(void) const {
    return partIndex_;
}

QList<Error*> PartProcessor::errors(void) const {
    return errors_;
}

void PartProcessor::pause(void) {
    paused_ = true;
}

void PartProcessor::cancel(void) {
    cancelled_ = true;
}

void PartProcessor::resume() {
    paused_ = false;
}

void PartProcessor::run(void) {
    if (!filesOpened) {
        LOG(ERROR) << "Files were not successfully opened. Cannot start " << (processType == kSplit ? "splitting" : "merging");
                return;
    }
    emit started(this);
    this->resume();
    int status = -1;
    if (processType == kSplit) {
        if (this->sourceFilename_ == "") {
            LOG(ERROR) << "Source file not specified";
            return;
        }
        status = split();
    } else if (processType == kMerge) {
        if (this->parts_.size() == 0) {
            LOG(ERROR) << "Parts not specified";
            return;
        }
        status = merge();
    }
    if (status == -1) {
        LOG(ERROR) << "Error occured while " << (processType == kSplit ? "splitting" : "merging");
    }
}
