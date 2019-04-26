#pragma once

#if defined(_MSC_VER)
# if defined(__RESTORE_MIN__)
__pragma(pop_macro("min"))
# undef __RESTORE_MIN__
# endif
# if defined(__RESTORE_MAX__)
__pragma(pop_macro("max"))
# undef __RESTORE_MAX__
# endif
#endif // defined(_MSC_VER)
