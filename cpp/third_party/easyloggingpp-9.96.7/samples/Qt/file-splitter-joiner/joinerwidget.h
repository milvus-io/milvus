#ifndef JOINERWIDGET_H
#define JOINERWIDGET_H

#include <QWidget>
namespace Ui {
class JoinerWidget;
}
class PartProcessor;
class JoinerCore;
class JoinerWidget : public QWidget {
    Q_OBJECT
    
public:
    explicit JoinerWidget(QWidget *parent = 0);
    ~JoinerWidget();
    
private slots:
    void on_buttonAddParts_clicked();

    void on_buttonUp_clicked();

    void on_buttonDown_clicked();

    void on_buttonStart_clicked();
    void updated(PartProcessor*);
    void finished(PartProcessor*);
    void started(PartProcessor*);
private:
    Ui::JoinerWidget *ui;
    JoinerCore* core;

};

#endif // JOINERWIDGET_H
