
/* libfiu - Fault Injection in Userspace
 *
 * This header, part of libfiu, is meant to be included in your project to
 * avoid having libfiu as a mandatory build-time dependency.
 *
 * You can add it to your project, and #include it instead of fiu.h.
 * The real fiu.h will be used only when FIU_ENABLE is defined.
 *
 * This header, as the rest of libfiu, is in the public domain.
 *
 * You can find more information about libfiu at
 * http://blitiri.com.ar/p/libfiu.
 */

#ifndef _FIU_LOCAL_H
#define _FIU_LOCAL_H

/* Only define the stubs when fiu is disabled, otherwise use the real fiu.h
 * header */
#ifndef FIU_ENABLE

#define fiu_init(flags) 0
#define fiu_fail(name) 0
#define fiu_failinfo() NULL
#define fiu_do_on(name, action)
#define fiu_exit_on(name)
#define fiu_return_on(name, retval)

#else

#include <fiu.h>

#endif /* FIU_ENABLE */

#endif /* _FIU_LOCAL_H */

