#include "window.h"
#include <gtkmm/main.h>
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

int main (int argc, char** argv) {
  START_EASYLOGGINGPP(argc, argv);
  el::Loggers::reconfigureAllLoggers(el::Level::Trace, el::ConfigurationType::Format, "%datetime %level Entering [%func]");

  Gtk::Main kit(argc, argv);

  Window win;
  Gtk::Main::run(win);

  return 0;
}
