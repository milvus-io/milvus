#include "splitablefiledelegate.h"
#include <QSpinBox>
#include <QStyleOptionProgressBarV2>
#include <QApplication>
#include <QDebug>
#include "splitterwidget.h"

SplitableFileDelegate::SplitableFileDelegate(SplitterWidget *parent)
    : QItemDelegate(parent),
      parent(parent) {
}

void SplitableFileDelegate::paint(QPainter *painter, const QStyleOptionViewItem &option, const QModelIndex &index) const {
    if (index.column() == 2) {
        float currProgress = (index.model()->data(index.model()->index(index.row(), 2))).toFloat();
        QStyleOptionProgressBarV2 progressBar;
        progressBar.rect = option.rect;
        progressBar.minimum = 0;
        progressBar.maximum = 100;
        progressBar.progress = currProgress;
        progressBar.text = QString::number(currProgress) + "%";
        progressBar.textVisible = true;

        QApplication::style()->drawControl(QStyle::CE_ProgressBar,
                                           &progressBar, painter);
    } else {
        QItemDelegate::paint(painter, option, index);
    }
}

QWidget* SplitableFileDelegate::createEditor(QWidget *parent,
                                             const QStyleOptionViewItem&,
                                             const QModelIndex& index ) const {
    if (index.column() == 3) {
        QSpinBox *editor = new QSpinBox(parent);
        editor->setMinimum(0);
        editor->setMaximum(SplitterWidget::kMaxSplitPossible);
        return editor;
    }
    return nullptr;
}

void SplitableFileDelegate::setEditorData(QWidget *editor,
                                          const QModelIndex &index) const {
    if (index.column() == 3) {
        int value = index.model()->data(index, Qt::EditRole).toInt();
        QSpinBox *spinBox = static_cast<QSpinBox*>(editor);
        spinBox->setMaximum(SplitterWidget::kMaxSplitPossible);
        spinBox->setMinimum(1);
        spinBox->setValue(value);
    }
}

void SplitableFileDelegate::setModelData(QWidget *editor, QAbstractItemModel *model,
                                         const QModelIndex &index) const {
    if (index.column() == 3) {
        QSpinBox *spinBox = static_cast<QSpinBox*>(editor);
        spinBox->interpretText();
        int value = spinBox->value();
        if (value != 0) {
            model->setData(index, value, Qt::EditRole);
            parent->updateSplitInfo(index);
        }
    }
}

void SplitableFileDelegate::updateEditorGeometry(QWidget *editor,
                                                 const QStyleOptionViewItem &option, const QModelIndex &/* index */) const {
    QRect rect = option.rect;
    rect.setHeight(rect.height() + 5);
    editor->setGeometry(rect);
}
