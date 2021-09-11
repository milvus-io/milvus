#ifndef __linux__
#    include "windows.h"
#else

#    include "sys/sysinfo.h"
#    include "unistd.h"
#endif

namespace NGT
{
int getCoreNumber();
}
