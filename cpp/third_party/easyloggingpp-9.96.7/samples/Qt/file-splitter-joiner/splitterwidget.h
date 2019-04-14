#ifndef SPLITTERWIDGET_H
#define SPLITTERWIDGET_H

#include <QWidget>
#include <QModelIndex>
#include <QList>

namespace Ui {
class SplitterWidget;
}

class QStandardItemModel;
class QTabWidget;
class QPushButton;
class SplitterCore;
class SplitableFileDelegate;
class PartProcessor;
class QProgressBar;

class SplitterWidget : public QWidget {
    Q_OBJECT
    
public:
    static const int kDefaultMaxParts = 5;
    static const int kMaxSplitPossible = 100;
    explicit SplitterWidget(QWidget *parent);
    ~SplitterWidget();

    void updateSplitInfo(const QModelIndex& index);

    QList<QProgressBar*> progressBars;
private slots:
    void on_buttonChooseFiles_clicked();
    void on_listFiles_doubleClicked(const QModelIndex &index);
    void on_listFiles_clicked(const QModelIndex &index);
    void on_buttonStartSplit_clicked();
    void cancelSplit(void) const;
    void pauseResume(void) const;
    void updated(PartProcessor*);
    void finished(PartProcessor*);
    void started(PartProcessor*);
    void fileStarted(int index, const QString& filename, const QString& destinationDir, int totalParts);
    void fileCompleted(int index);
    void on_buttonRemove_clicked();

private:
    Ui::SplitterWidget *ui;
    QPushButton* buttonRemove;
    QStandardItemModel* fileModel;
    QTabWidget* parent;
    QPushButton* cancelButton;
    QPushButton* pauseResumeButton;
    SplitterCore* splitter;
    SplitableFileDelegate* splitDelegate;
    void startSplitting(void);
    void updateRemoveButtonUI(void) const;
};

#endif // SPLITTERWIDGET_H
