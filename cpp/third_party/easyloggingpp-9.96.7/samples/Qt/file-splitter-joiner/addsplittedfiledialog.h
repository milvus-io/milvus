#ifndef ADDSPLITTEDFILEDIALOG_H
#define ADDSPLITTEDFILEDIALOG_H

#include <QDialog>

namespace Ui {
class AddSplittedFileDialog;
}

class QTreeWidget;
class QTreeWidgetItem;
class QDialogButtonBox;

class AddSplittedFileDialog : public QDialog {
    Q_OBJECT
    
public:
    explicit AddSplittedFileDialog(QTreeWidget* parentView, QWidget *parent = 0);
    ~AddSplittedFileDialog();
    
private slots:
    void on_buttonAddPart_clicked();
    void on_buttonBox_accepted();

    void on_pushButton_clicked();

    void on_buttonRemove_clicked();

private:
    Ui::AddSplittedFileDialog *ui;
    QTreeWidgetItem* filenamePointer;
    QTreeWidgetItem* filePartsPointers;
    QTreeWidget* parentView;
    QDialogButtonBox* buttonOkCancel;
};

#endif // ADDSPLITTEDFILEDIALOG_H
