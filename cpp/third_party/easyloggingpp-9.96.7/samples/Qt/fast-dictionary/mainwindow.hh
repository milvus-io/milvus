#ifndef MAINWINDOW_HH
#define MAINWINDOW_HH

#include <QMainWindow>

namespace Ui {
class MainWindow;
}

class ListWithSearch;

class MainWindow : public QMainWindow
{
    Q_OBJECT
    
public:
    explicit MainWindow(QWidget *parent = 0);
    ~MainWindow();

    /**
     * Loads memory from list of words provided as param.
     * Note, in the source file, this function has performance tracking on.
     */
    void initializeDictionary(const QString& wordsFile);
    void resizeEvent(QResizeEvent *);
public slots:
    void onSelectionMade(const QString& word);

private slots:
    void on_buttonInfo_clicked();

private:
    Ui::MainWindow* ui;
    ListWithSearch* list;
};

#endif // MAINWINDOW_HH
