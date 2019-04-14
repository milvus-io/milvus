#ifndef JOINERCORE_H
#define JOINERCORE_H

#include <QObject>

class QTreeWidget;
class PartProcessor;

class JoinerCore : public QObject {
    Q_OBJECT
public:
    explicit JoinerCore(QTreeWidget* widget);
    void startJoining(void);
private:
    QTreeWidget* widget;
    PartProcessor* core;
signals:
    void updated(PartProcessor*) const;
    void started(PartProcessor*) const;
    void finished(PartProcessor*) const;
};

#endif // JOINERCORE_H
