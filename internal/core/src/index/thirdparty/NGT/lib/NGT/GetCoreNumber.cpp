#include "NGT/GetCoreNumber.h"

namespace NGT
{
int getCoreNumber()
{
#ifndef __linux__
    SYSTEM_INFO sys_info;
    GetSystemInfo(&sys_info);
    return sys_info.dwNumberOfProcessors;
#else
    return get_nprocs();
#endif
}
}
