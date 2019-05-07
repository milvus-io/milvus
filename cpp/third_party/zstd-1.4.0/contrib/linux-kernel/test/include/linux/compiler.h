#ifndef LINUX_COMPILER_H_
#define LINUX_COMPILER_H_

#ifndef __always_inline
#  define __always_inline inline
#endif

#ifndef noinline
#  define noinline __attribute__((__noinline__))
#endif

#endif // LINUX_COMPILER_H_
