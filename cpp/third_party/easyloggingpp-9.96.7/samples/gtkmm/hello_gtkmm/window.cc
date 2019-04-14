#include "window.h"
#include <iostream>
#include "easylogging++.h"

Window::Window() 
    : m_button("Click Me")   {
  LOG(TRACE);

  set_border_width(10);

  m_button.signal_clicked().connect(sigc::mem_fun(*this, &Window::on_button_clicked));

  m_button.show();
  add(m_button);
}

Window::~Window() {
    LOG(TRACE);
}

void Window::on_button_clicked() {
  LOG(TRACE);
  LOG(INFO) << "Button has been clicked!";
}

