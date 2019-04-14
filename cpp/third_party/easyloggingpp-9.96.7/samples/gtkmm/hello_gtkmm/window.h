#ifndef GTKMM_EXAMPLE_Window_H
#define GTKMM_EXAMPLE_Window_H

#include <gtkmm/button.h>
#include <gtkmm/window.h>

class Window : public Gtk::Window
{

public:
  Window();
  virtual ~Window();

protected:
  //Signal handlers:
  void on_button_clicked();

  //Member widgets:
  Gtk::Button m_button;
};

#endif // GTKMM_EXAMPLE_Window_H
