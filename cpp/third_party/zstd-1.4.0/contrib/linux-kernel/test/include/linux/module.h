#ifndef LINUX_MODULE_H_
#define LINUX_MODULE_H_

#define EXPORT_SYMBOL(symbol)                                                  \
  void* __##symbol = symbol
#define MODULE_LICENSE(license) static char const *const LICENSE = license
#define MODULE_DESCRIPTION(description)                                        \
  static char const *const DESCRIPTION = description

#endif // LINUX_MODULE_H_
