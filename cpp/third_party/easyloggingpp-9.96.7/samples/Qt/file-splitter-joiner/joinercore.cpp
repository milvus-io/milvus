#include "joinercore.h"
#include <QTreeWidget>
#include <QModelIndex>
#include "partprocessor.h"

JoinerCore::JoinerCore(QTreeWidget* widget) {
    this->widget = widget;
    core = nullptr;
}

void JoinerCore::startJoining(void) {
    QList<QString> parts;
    QString filename = "";
    for (int i = 0; i < widget->topLevelItemCount(); i++) {
        if (core) {
            delete core;
            core = nullptr;
        }
        QTreeWidgetItem* currentFile = widget->topLevelItem(i);
        parts.clear();
        filename = currentFile->text(0);
        for (int p = 0; p < currentFile->childCount(); p++) {
            parts.append(currentFile->child(p)->text(0));
        }
        core = new PartProcessor(parts, filename, QModelIndex(), PartProcessor::kMerge, this);
        connect(core, SIGNAL(updated(PartProcessor*)), this, SIGNAL(updated(PartProcessor*)));
        connect(core, SIGNAL(started(PartProcessor*)), this, SIGNAL(started(PartProcessor*)));
        connect(core, SIGNAL(finished(PartProcessor*)), this, SIGNAL(finished(PartProcessor*)));
        core->start();
        emit started(core);
        core->wait();
        emit finished(core);
    }
}
