#pragma once

#if defined(_MSC_VER)
# if defined(min)
__pragma(push_macro("min"))
# undef min
# define __RESTORE_MIN__
# endif
# if defined(max)
__pragma(push_macro("max"))
# undef max
# define __RESTORE_MAX__
# endif
#endif // defined(_MSC_VER)

#include <ciso646>  //  due to #166

