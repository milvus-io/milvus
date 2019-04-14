#ifndef SPLITTER_H
#define SPLITTER_H

#include <QThread>
#include <QList>

class QModelIndex;
class QStandardItemModel;
class PartProcessor;
class SplitterWidget;

class SplitterCore : public QThread {
    Q_OBJECT
public:
    SplitterCore(QStandardItemModel* fileModel, QObject* parent = 0, SplitterWidget* parentWidget = 0);
    void run(void);

    bool paused(void) const;
    void cancel(void);
    void pause(void);
    void resume(void);
private:
    QStandardItemModel* fileModel;
    QList<PartProcessor*> splitJoinCores;
    bool cancelled;
    SplitterWidget* parent;
signals:
    void updated(PartProcessor*) const;
    void started(PartProcessor*) const;
    void finished(PartProcessor*) const;
    void fileStarted(int index, const QString& filename, const QString& destinationDir, int totalParts) const;
    void fileCompleted(int index) const;
};

#endif // SPLITTER_H
