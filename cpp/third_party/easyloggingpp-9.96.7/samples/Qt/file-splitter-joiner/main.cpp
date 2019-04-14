#include <QApplication>
#include <QStandardItemModel>
#include <QFileInfo>
#include "mainwindow.h"
#include "easylogging++.h"
#include "splittercore.h"
#include "joinercore.h"
#include "partprocessor.h"

INITIALIZE_EASYLOGGINGPP

void split(int argc, char** argv);
void merge(int argc, char** argv);

int main(int argc, char** argv) {
    START_EASYLOGGINGPP(argc, argv);

    // TODO: Use getopt with following params
    //         -p : process_type
    //         -s : source_file (for split only)
    //         -d : destination_dir / destination_file (depending on process_type)
    //         -t : total_parts (for split only)
    //       For [parts...] in join we just force user to have last arguments to be all parts

    if (argc > 2) {
        int status = -1;
        if (strcmp(argv[1], "split") == 0) {
            //use splitter core
            split(argc - 2, argv + 2);
        } else if ((strcmp(argv[1], "merge") == 0) || (strcmp(argv[1], "join") == 0)) {
            //use merger core
            merge(argc - 2, argv + 2);
        } else {
            LOG(ERROR) << "Invalid process type!";
        }
        return status;
    } else {
        QApplication app(argc, argv);
        MainWindow w;
        w.show();
        return app.exec();
    }
}

void help(PartProcessor::kProcessType type) {
    if (type == PartProcessor::kSplit) {
        LOG(INFO) << "split [source_file] [total_parts] [destination_dir]";
    } else {
        LOG(INFO) << "join [destination_file] [parts...]";
    }
}

void split(int argc, char** argv) {
    //Syntax: split [source_file] [total_parts] [destination_dir]
    if (argc >= 3) {
        QStandardItemModel* fileModel = new QStandardItemModel(1, 6);
        fileModel->deleteLater();
        SplitterCore* core = new SplitterCore(fileModel);
        core->deleteLater();
        //TODO: connect signals and implement them to show progress in terminal
        QString filePath(argv[0]);
        int parts = QString(argv[1]).toInt();
        QString destPath(argv[2]);
        QFileInfo fileinfo(filePath);
        if (fileinfo.isFile()) {
            fileModel->setData(fileModel->index(0, 0), filePath);
            fileModel->setData(fileModel->index(0, 1), destPath);
            fileModel->setData(fileModel->index(0, 2), 0);
            fileModel->setData(fileModel->index(0, 3), parts);
            fileModel->setData(fileModel->index(0, 4), fileinfo.size());
        }
        core->start();
        core->wait();
    } else {
        help(PartProcessor::kSplit);
    }
}

void merge(int argc, char** argv) {
    //Syntax: join [destination_file] [parts...]
    if (argc >= 2) {
        QString destinationFile(argv[0]);
        argc--;
        ++*argv;
        QList<QString> parts;
        int index = 0;
        while (index != argc) {
            parts << QString(argv[index]);
            index++;
        }
        PartProcessor *core = new PartProcessor(parts, destinationFile, QModelIndex(), PartProcessor::kMerge);
        core->deleteLater();
        //TODO: connect signals and implement them to show progress in terminal
        core->start();
        core->wait();
    } else {
        help(PartProcessor::kMerge);
    }
}
