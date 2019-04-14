////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "SignalUtil.h"
#include "CommonUtil.h"
#include "server/Server.h"

#include <signal.h>
#include <execinfo.h>

namespace zilliz {
namespace vecwise {
namespace server {

void SignalUtil::HandleSignal(int signum){
    CommonUtil::PrintInfo("Server received signal:" + std::to_string(signum));

    switch(signum){
        case SIGUSR2:{
            server::Server* server_ptr = server::Server::Instance();
            server_ptr->Stop();

            exit(0);

        }
        default:{
            SignalUtil::PrintStacktrace();

            std::string info = "Server encounter critical signal:";
            info += std::to_string(signum);
//            SendSignalMessage(signum, info);

            CommonUtil::PrintInfo(info);

            server::Server* server_ptr = server::Server::Instance();
            server_ptr->Stop();

            exit(1);
        }
    }
}

void SignalUtil::PrintStacktrace() {
    CommonUtil::PrintInfo("Call stack:");

    const int size = 32;
    void* array[size];
    int stack_num = backtrace(array, size);
    char ** stacktrace = backtrace_symbols(array, stack_num);
    for (int i = 0; i < stack_num; ++i) {
        std::string info = stacktrace[i];
        CommonUtil::PrintInfo(info);
    }
    free(stacktrace);
}


}
}
}
