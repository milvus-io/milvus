###### File splitter/joiner sample

**About**

This is multi-threaded application that uses threads in order to split/merge part/s. The idea is not to show how to use threading in Qt, in fact, I might have done it wrong ([this document](http://qt-project.org/wiki/Threads_Events_QObjects) can be helpful to make thread-use better), the idea behind this sample is to show you a possible usage of Easylogging++ is fairly large scale i.e, multiple files project using multi-threading.

**Usage**

Once you successfully compile the project using minimum of Qt 4.6.2, you can use this in two ways;

 * Using command-line

   **Split**: `./file-splitter-joiner split [source_file] [total_parts] [destination_dir]`

   **Join**:  `./file-splitter-joiner join [destination_file] [parts...]`

 * Using GUI

        When you don't provide enough parameters, a GUI based program will be launched

**Screen Shots**

   [![Splitter](http://easylogging.org/images/screenshots/splitter.png)](http://easylogging.org/images/screenshots/splitter.png)

   [![Joiner](http://easylogging.org/images/screenshots/joiner.png)](http://easylogging.org/images/screenshots/joiner.png)
