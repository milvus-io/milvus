#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>

class QTabWidget;
class SplitterWidget;
class JoinerWidget;
class About;

class MainWindow : public QMainWindow {
    Q_OBJECT
    
public:
    const static int kWidth     =   550;
    const static int kHeight    =   400;
    MainWindow(QWidget *parent = 0);
    QTabWidget* tabWidget;
    ~MainWindow();
private:
    SplitterWidget* splitterWidget;
    JoinerWidget* joinerWidget;
    About* about;

    void setupUi(void);
    void initWidgets(void);
};

#endif // MAINWINDOW_H
