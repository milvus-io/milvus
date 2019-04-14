#ifndef SPLITJOINCORE_H
#define SPLITJOINCORE_H

#include <QThread>
#include <QFile>
#include <QList>
#include <QMutex>
#include <QModelIndex>

class Error {
public:
    explicit Error(const QString& message, int number) : message(message), number(number) {}
    QString message;
    int number;
};

class PartProcessor : public QThread
{
    Q_OBJECT
public:
    static const char* kPartSuffix;
    enum kErrorTypes {
        kOpenFileError = 13
    };
    enum kProcessType {
        kSplit = 0,
        kMerge = 1
    };

    explicit PartProcessor(const QString& sourceFilename_, const QString& destination, qint32 startOffset, qint32 maxSizePerPart_, qint32 originalFileSize_, int partIndex, const QModelIndex& index, kProcessType processType, QObject *parent = 0);
    explicit PartProcessor(const QList<QString>& parts, const QString& destination, const QModelIndex& index, kProcessType processType, QObject *parent = 0);
    ~PartProcessor(void);
    virtual void run(void);
    bool paused(void) const;
    void pause(void);
    void resume(void);
    void cancel(void);
    QModelIndex& modelIndex(void) const;
    int partIndex(void) const;
    qint32 progress(void) const;
    qint32 originalFileSize(void) const;
    QList<Error*> errors(void) const;
private:
    static const int kBufferSize           = 4096;
    static const int kUpdateFrequencyBytes = 2000;
    bool cancelled_;
    bool paused_;
    kProcessType processType;
    QMutex mutex_;
    QString sourceFilename_;
    QString destinationFilename_;
    qint32 seekLocation_;
    qint32 maxSizePerPart_;
    qint32 originalFileSize_;
    QFile *sourceFile_;
    QFile destinationFile_;
    QList<Error*> errors_;
    QList<QString> parts_;
    QModelIndex& modelIndex_;
    int partIndex_;
    qint32 progress_;
    bool filesOpened;

    void closeFiles(void);
    void openFiles(void);
    int split(void);
    int merge(void);
signals:
    void started(PartProcessor* core);
    void updated(PartProcessor* core);
    void finished(PartProcessor* core);
public slots:

};

#endif // SPLITJOINCORE_H
